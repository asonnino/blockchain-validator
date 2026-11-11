// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Sui's latest execution cut behind [`ExecutionEngine`].

use std::{collections::HashMap, sync::Arc};

use blake2::{Blake2b, Digest as _, digest::consts::U32};
use execution::{
    effects::{AbortReason, Event, ExecutionOutput},
    engine::ExecutionEngine,
    object::{Object, ObjectId, Version},
    store::{InMemoryStore, StateView},
    transaction::{AccessMode, FunctionId, Transaction},
};
use move_binary_format::{CompiledModule, file_format::Visibility};
use move_core_types::identifier::Identifier;
use move_vm_runtime::runtime::MoveRuntime;
use prometheus::Registry;
use serde::{Deserialize, Serialize};
use sui_adapter_latest::{
    adapter::new_move_runtime, execution_engine::execute_transaction_to_effects,
    execution_mode::Normal,
};
use sui_framework::BuiltInFramework;
use sui_move_natives_latest::all_natives;
use sui_protocol_config::{Chain, ProtocolConfig, ProtocolVersion};
use sui_types::{
    MOVE_STDLIB_PACKAGE_ID, SUI_FRAMEWORK_PACKAGE_ID,
    accumulator_root::EmptyUnsettledObjectFunds,
    base_types::{ObjectID, SequenceNumber, SuiAddress, SystemObjectVersions},
    committee::EpochId,
    digests::{ObjectDigest, TransactionDigest},
    effects::{IDOperation, TransactionEffects, TransactionEffectsAPI},
    execution_params::ExecutionOrEarlyError,
    gas::SuiGasStatus,
    inner_temporary_store::InnerTemporaryStore,
    metrics::ExecutionMetrics,
    object::{Object as SuiObject, Owner},
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{
        CheckedInputObjects, GasData, InputObjectKind, InputObjects, ObjectArg, ObjectReadResult,
        ObjectReadResultKind, ProgrammableTransaction, SharedObjectMutability, TransactionKind,
    },
};

use crate::store::SuiView;

/// The execution cut this engine calls, which the pinned protocol version must select.
const LATEST_EXECUTION_VERSION: u64 = 4;
/// Sends every transaction and owns every address-owned object. Not `0x0`, which Sui reserves for
/// system transactions.
const SENDER: ObjectID = ObjectID::from_single_byte(0x5e);
/// Every validator must compute the same result, so the epoch and its clock are fixed.
const EPOCH: EpochId = 0;
const EPOCH_TIMESTAMP_MS: u64 = 0;

/// The arguments of a call, BCS-encoded in its [`Transaction`]'s `args`.
#[derive(Serialize, Deserialize)]
pub struct MoveCall {
    pub arguments: Vec<MoveArgument>,
}

#[derive(Serialize, Deserialize)]
pub enum MoveArgument {
    /// A BCS-encoded pure value.
    Pure(Vec<u8>),
    /// The transaction input at this index.
    Object(u16),
}

/// Executes one entry-function call per transaction through Sui's adapter, unmetered.
pub struct MoveEngine {
    protocol_config: ProtocolConfig,
    runtime: Arc<MoveRuntime>,
    metrics: Arc<ExecutionMetrics>,
    /// Sui's framework, then the configured packages and the objects their `init` functions made.
    genesis: Vec<SuiObject>,
    packages: Vec<ObjectID>,
    functions: HashMap<FunctionId, (ObjectID, Identifier, Identifier)>,
}

impl MoveEngine {
    /// Publishes `packages`, each a list of compiled modules depending only on Move's standard
    /// library and the Sui framework, at genesis on top of Sui's framework.
    pub fn new(packages: Vec<Vec<Vec<u8>>>) -> Self {
        let protocol_config = ProtocolConfig::get_for_version(ProtocolVersion::MAX, Chain::Unknown);
        assert_eq!(
            protocol_config.execution_version(),
            LATEST_EXECUTION_VERSION,
            "the pinned protocol version must select the latest execution cut"
        );
        let natives = all_natives(true, &protocol_config);
        let runtime = new_move_runtime(natives, &protocol_config).expect("the Move runtime builds");
        let mut engine = Self {
            protocol_config,
            runtime: Arc::new(runtime),
            metrics: Arc::new(ExecutionMetrics::new(&Registry::new())),
            genesis: BuiltInFramework::genesis_objects().collect(),
            packages: Vec::new(),
            functions: HashMap::new(),
        };

        let mut store = InMemoryStore::default();
        store.apply(ExecutionOutput::success(
            engine.genesis.iter().map(to_object).collect(),
        ));
        for (index, modules) in packages.into_iter().enumerate() {
            let mut builder = ProgrammableTransactionBuilder::new();
            builder.publish_immutable(
                modules,
                vec![MOVE_STDLIB_PACKAGE_ID, SUI_FRAMEWORK_PACKAGE_ID],
            );
            // Fixed, so every validator derives the same package and object ids.
            let digest = TransactionDigest::new(
                Blake2b::<U32>::digest(format!("genesis package {index}")).into(),
            );
            let (inner_store, effects) =
                engine.run(&SuiView(&store), builder.finish(), Vec::new(), digest);
            assert!(
                effects.status().is_ok(),
                "genesis package {index} must publish: {:?}",
                effects.status()
            );
            let written: Vec<_> = inner_store.written.into_values().collect();
            store.apply(ExecutionOutput::success(
                written.iter().map(to_object).collect(),
            ));
            let published = written.iter().filter(|object| object.is_package());
            engine.packages.extend(published.map(|object| object.id()));
            engine.genesis.extend(written);
        }
        engine.functions = engine.index_functions();
        engine
    }

    /// The configured packages' ids, in publication order.
    pub fn packages(&self) -> &[ObjectID] {
        &self.packages
    }

    /// The id a [`Transaction`] uses to call `package::module::function`.
    pub fn function_id(package: ObjectID, module: &str, function: &str) -> FunctionId {
        let name = format!("{package}::{module}::{function}");
        FunctionId::from_bytes(Blake2b::<U32>::digest(name).into())
    }

    /// The id of the first `UID` that `transaction`'s call makes with `object::new`.
    pub fn fresh_id(transaction: &Transaction) -> ObjectId {
        let id = ObjectID::derive_id(sui_digest(transaction), 0);
        ObjectId::from_bytes(id.into_bytes())
    }

    pub(crate) fn genesis_objects(&self) -> &[SuiObject] {
        &self.genesis
    }

    /// Every public or entry function of the genesis packages, by [`Self::function_id`]. Calls
    /// carry no type arguments, so generic functions are left out.
    fn index_functions(&self) -> HashMap<FunctionId, (ObjectID, Identifier, Identifier)> {
        let mut functions = HashMap::new();
        for package in self
            .genesis
            .iter()
            .filter_map(|object| object.data.try_as_package())
        {
            for bytes in package.serialized_module_map().values() {
                let module = CompiledModule::deserialize_with_defaults(bytes)
                    .expect("published modules deserialize");
                for definition in module.function_defs() {
                    if !definition.is_entry && definition.visibility != Visibility::Public {
                        continue;
                    }
                    let handle = module.function_handle_at(definition.function);
                    if !handle.type_parameters.is_empty() {
                        continue;
                    }
                    let name = module.identifier_at(handle.name).to_owned();
                    let module_name = module.name().to_owned();
                    let id = Self::function_id(package.id(), module_name.as_str(), name.as_str());
                    functions.insert(id, (package.id(), module_name, name));
                }
            }
        }
        functions
    }

    /// Runs a programmable transaction from [`SENDER`] through Sui's adapter.
    fn run<V: StateView>(
        &self,
        store: &SuiView<'_, V>,
        transaction: ProgrammableTransaction,
        inputs: Vec<ObjectReadResult>,
        digest: TransactionDigest,
    ) -> (InnerTemporaryStore, TransactionEffects) {
        let sender = SuiAddress::from(SENDER);
        // The sentinel payment Sui reads as unmetered: no gas coin is needed, and nothing is
        // charged against the price or budget.
        let gas_data = GasData {
            payment: vec![(ObjectID::ZERO, SequenceNumber::default(), ObjectDigest::MIN)],
            owner: sender,
            price: 1,
            budget: self.protocol_config.max_tx_gas(),
        };
        let output = execute_transaction_to_effects::<Normal>(
            store,
            CheckedInputObjects::new_with_checked_transaction_inputs(InputObjects::new(inputs)),
            SystemObjectVersions::empty(),
            &EmptyUnsettledObjectFunds,
            gas_data,
            SuiGasStatus::new_unmetered(&self.protocol_config),
            TransactionKind::ProgrammableTransaction(transaction),
            None,
            sender,
            digest,
            &self.runtime,
            &EPOCH,
            EPOCH_TIMESTAMP_MS,
            &self.protocol_config,
            self.metrics.clone(),
            false,
            ExecutionOrEarlyError::ok(None),
            &mut None,
        );
        (output.inner_store, output.effects)
    }
}

impl ExecutionEngine for MoveEngine {
    fn genesis(&self) -> Vec<Object> {
        self.genesis.iter().map(to_object).collect()
    }

    fn execute<V: StateView>(&self, view: &V, transaction: &Transaction) -> ExecutionOutput {
        let Some((package, module, function)) = self.functions.get(&transaction.function()) else {
            return ExecutionOutput::aborted(AbortReason::UnknownFunction);
        };
        let Ok(call) = bcs::from_bytes::<MoveCall>(transaction.args()) else {
            return ExecutionOutput::aborted(AbortReason::ExecutionFailure);
        };

        let store = SuiView(view);
        let mut builder = ProgrammableTransactionBuilder::new();
        let mut inputs = Vec::with_capacity(transaction.inputs().len());
        let mut objects = Vec::with_capacity(transaction.inputs().len());
        for (id, mode) in transaction.inputs() {
            // Move derives created ids from the transaction digest, so it declares no creations.
            if *mode == AccessMode::WriteOnly {
                return ExecutionOutput::aborted(AbortReason::ExecutionFailure);
            }
            let id = ObjectID::new(*id.as_bytes());
            let Some(object) = store.object(&id) else {
                return ExecutionOutput::aborted(AbortReason::MissingRead);
            };
            // Calls cannot take packages or child objects, which Sui's adapter asserts never
            // reach it.
            let (argument, kind) = match object.owner {
                Owner::Shared {
                    initial_shared_version,
                }
                | Owner::ConsensusAddressOwner {
                    start_version: initial_shared_version,
                    ..
                }
                | Owner::Party {
                    start_version: initial_shared_version,
                    ..
                } => {
                    let mutability = match mode {
                        AccessMode::ReadWrite => SharedObjectMutability::Mutable,
                        _ => SharedObjectMutability::Immutable,
                    };
                    let argument = ObjectArg::SharedObject {
                        id,
                        initial_shared_version,
                        mutability,
                    };
                    let kind = InputObjectKind::SharedMoveObject {
                        id,
                        initial_shared_version,
                        mutability,
                    };
                    (argument, kind)
                }
                // Sui rewrites every owned input, so the call must declare it writable.
                Owner::AddressOwner(_) if *mode != AccessMode::ReadWrite => {
                    return ExecutionOutput::aborted(AbortReason::ExecutionFailure);
                }
                Owner::AddressOwner(_) | Owner::Immutable if !object.is_package() => {
                    let reference = object.compute_object_reference();
                    let kind = InputObjectKind::ImmOrOwnedMoveObject(reference);
                    (ObjectArg::ImmOrOwnedObject(reference), kind)
                }
                _ => return ExecutionOutput::aborted(AbortReason::ExecutionFailure),
            };
            objects.push(builder.obj(argument).expect("inputs are unique"));
            inputs.push(ObjectReadResult {
                input_object_kind: kind,
                object: ObjectReadResultKind::Object(object),
            });
        }
        let mut arguments = Vec::with_capacity(call.arguments.len());
        for argument in call.arguments {
            arguments.push(match argument {
                MoveArgument::Pure(bytes) => builder.pure_bytes(bytes, false),
                MoveArgument::Object(index) => match objects.get(usize::from(index)) {
                    Some(object) => *object,
                    None => return ExecutionOutput::aborted(AbortReason::ExecutionFailure),
                },
            });
        }
        builder.programmable_move_call(
            *package,
            module.clone(),
            function.clone(),
            Vec::new(),
            arguments,
        );

        let digest = sui_digest(transaction);
        let (inner_store, effects) = self.run(&store, builder.finish(), inputs, digest);
        if !effects.status().is_ok() {
            return ExecutionOutput::aborted(AbortReason::ExecutionFailure);
        }
        let changes = effects.object_changes();
        // A replayed transaction derives the same fresh ids again, those it wraps at once
        // included. Dynamic fields derive theirs from their parent and key instead, so a removed
        // field may come back.
        let reused = changes
            .iter()
            .filter(|change| change.id_operation == IDOperation::Created)
            .any(|change| {
                let object = inner_store.written.get(&change.id);
                let field = object.and_then(|object| object.type_());
                !field.is_some_and(|ty| ty.is_dynamic_field())
                    && view
                        .latest_version(&ObjectId::from_bytes(change.id.into_bytes()))
                        .is_some()
            });
        if reused {
            return ExecutionOutput::aborted(AbortReason::IdInUse);
        }
        let writes = inner_store.written.values().map(to_object).collect();
        // Deleted and wrapped objects, fresh ones included, leave tombstones that keep their ids
        // in use.
        let version = Version::new(effects.lamport_version().value());
        let deletes = changes
            .iter()
            .filter(|change| change.output_version.is_none())
            .map(|change| (ObjectId::from_bytes(change.id.into_bytes()), version))
            .collect();
        let events = inner_store
            .events
            .data
            .iter()
            .map(|event| Event::new(bcs::to_bytes(event).expect("events serialize")))
            .collect();
        ExecutionOutput::success(writes)
            .with_deletes(deletes)
            .with_events(events)
    }
}

fn sui_digest(transaction: &Transaction) -> TransactionDigest {
    TransactionDigest::new(*transaction.digest().as_bytes())
}

fn to_object(object: &SuiObject) -> Object {
    let id = ObjectId::from_bytes(object.id().into_bytes());
    let contents = bcs::to_bytes(object).expect("objects serialize");
    Object::new(id, Version::new(object.version().value()), contents)
}
