// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Sui's latest execution cut behind [`ExecutionEngine`].

use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    rc::Rc,
    sync::Arc,
};

use blake2::{Blake2b, Digest as _, digest::consts::U32};
use execution::{
    effects::{AbortReason, Event, ExecutionOutput, FailureDetail},
    engine::{CommitInfo, ExecutionEngine},
    object::{Object, ObjectId, Version},
    store::{InMemoryStore, StateView},
    transaction::{AccessMode, FunctionId, Transaction},
};
use eyre::{bail, eyre};
use move_binary_format::{
    CompiledModule, compatibility::Compatibility, errors::VMError, file_format::Visibility,
    normalized::RcPool,
};
use move_core_types::{ident_str, identifier::Identifier};
use move_vm_runtime::{
    natives::extensions::NativeContextExtensions, runtime::MoveRuntime,
    shared::gas::UnmeteredGasMeter,
};
use prometheus::Registry;
use serde::{Deserialize, Serialize};
use sui_adapter_latest::{
    adapter::{new_move_runtime, substitute_package_id},
    data_store::transaction_package_store::TransactionPackageStore,
    execution_engine::{ExecutionOutput as SuiOutput, execute_transaction_to_effects},
    execution_mode::{ExecutionMode, Genesis, Normal},
};
use sui_framework::BuiltInFramework;
use sui_move_natives_latest::all_natives;
use sui_protocol_config::{Chain, ProtocolConfig, ProtocolVersion};
use sui_types::{
    MOVE_STDLIB_PACKAGE_ID, SUI_CLOCK_OBJECT_ID, SUI_CLOCK_OBJECT_SHARED_VERSION,
    SUI_FRAMEWORK_PACKAGE_ID,
    accumulator_root::EmptyUnsettledObjectFunds,
    base_types::{
        ConsensusObjectVersion, ObjectID, SequenceNumber, SuiAddress, SystemObjectVersions,
    },
    clock::CLOCK_MODULE_NAME,
    committee::EpochId,
    digests::{ObjectDigest, TransactionDigest},
    effects::{IDOperation, TransactionEffectsAPI},
    error::ExecutionError,
    execution_params::ExecutionOrEarlyError,
    execution_status::ExecutionStatus as SuiStatus,
    gas::SuiGasStatus,
    messages_consensus::ConsensusCommitPrologue,
    metrics::ExecutionMetrics,
    move_package::{MovePackage, normalize_deserialized_modules},
    object::{Object as SuiObject, Owner},
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    storage::PackageObject,
    transaction::{
        CheckedInputObjects, GasData, InputObjectKind, InputObjects, ObjectArg, ObjectReadResult,
        ObjectReadResultKind, ProgrammableTransaction, SharedObjectMutability, TransactionKind,
    },
};
use sui_verifier_latest::verifier::sui_verify_module_unmetered;

use crate::{
    failure::MoveFailure,
    store::{GenesisPackages, SuiView},
};

/// The execution cut this engine calls, which the pinned protocol version must select.
const LATEST_EXECUTION_VERSION: u64 = 4;
/// Sends every transaction and owns every address-owned object. Not `0x0`, which Sui reserves for
/// system transactions.
const SENDER: ObjectID = ObjectID::from_single_byte(0x5e);
/// The validator runs a single epoch, so `TxContext`'s epoch and its start time are fixed.
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
    /// Sui's framework and clock, then the configured packages and the objects their `init`
    /// functions made.
    genesis: Vec<SuiObject>,
    packages: Vec<ObjectID>,
    functions: HashMap<FunctionId, (ObjectID, Identifier, Identifier)>,
    substitutes: HashMap<ObjectID, PackageObject>,
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
        let mut engine = Self {
            runtime: runtime(&protocol_config),
            protocol_config,
            metrics: Arc::new(ExecutionMetrics::new(&Registry::new())),
            genesis: BuiltInFramework::genesis_objects().collect(),
            packages: Vec::new(),
            functions: HashMap::new(),
            substitutes: HashMap::new(),
        };

        let mut store = InMemoryStore::default();
        store.apply(ExecutionOutput::success(
            engine.genesis.iter().map(to_object).collect(),
        ));
        // Sui's clock, which only the system address may create.
        let mut builder = ProgrammableTransactionBuilder::new();
        builder.programmable_move_call(
            SUI_FRAMEWORK_PACKAGE_ID,
            CLOCK_MODULE_NAME.to_owned(),
            ident_str!("create").to_owned(),
            Vec::new(),
            Vec::new(),
        );
        engine.genesis_transaction::<Genesis>(
            &mut store,
            builder.finish(),
            SuiAddress::ZERO,
            "genesis clock",
        );
        for (index, modules) in packages.into_iter().enumerate() {
            let mut builder = ProgrammableTransactionBuilder::new();
            builder.publish_immutable(
                modules,
                vec![MOVE_STDLIB_PACKAGE_ID, SUI_FRAMEWORK_PACKAGE_ID],
            );
            let name = format!("genesis package {index}");
            let published: Vec<_> = engine
                .genesis_transaction::<Normal>(
                    &mut store,
                    builder.finish(),
                    SuiAddress::from(SENDER),
                    &name,
                )
                .iter()
                .filter(|object| object.is_package())
                .map(|object| object.id())
                .collect();
            engine.packages.extend(published);
        }
        engine.functions = engine.index_functions();
        engine
    }

    /// Runs `transaction` from `sender` at genesis, applying its writes to `store` and adding
    /// them to the genesis objects, which it returns. `name` fixes the digest, so every
    /// validator derives the same ids.
    fn genesis_transaction<Mode: ExecutionMode>(
        &mut self,
        store: &mut InMemoryStore,
        transaction: ProgrammableTransaction,
        sender: SuiAddress,
        name: &str,
    ) -> &[SuiObject] {
        let digest = TransactionDigest::new(Blake2b::<U32>::digest(name).into());
        let kind = TransactionKind::ProgrammableTransaction(transaction);
        let SuiOutput {
            inner_store,
            effects,
            ..
        } = self.run::<Mode, _>(
            &SuiView(&*store, &self.substitutes),
            kind,
            sender,
            Vec::new(),
            digest,
        );
        assert!(
            effects.status().is_ok(),
            "{name} must succeed: {:?}",
            effects.status()
        );
        let start = self.genesis.len();
        self.genesis.extend(inner_store.written.into_values());
        let written = &self.genesis[start..];
        store.apply(ExecutionOutput::success(
            written.iter().map(to_object).collect(),
        ));
        written
    }

    /// The configured packages' ids, in publication order.
    pub fn packages(&self) -> &[ObjectID] {
        &self.packages
    }

    /// Runs `modules` in place of the configured `package`, so engines can run different
    /// implementations over the same state. The substitute is verified as at publication and
    /// must keep the published modules, datatype layouts, and public and entry function
    /// signatures, so objects stay readable across implementations; bodies and private functions
    /// may differ.
    pub fn substitute(&mut self, package: ObjectID, modules: Vec<Vec<u8>>) -> eyre::Result<()> {
        if !self.packages.contains(&package) {
            bail!("{package} is not a configured package");
        }
        let published = self
            .genesis
            .iter()
            .find(|object| object.id() == package)
            .expect("genesis holds the configured packages");
        let published_package = published
            .data
            .try_as_package()
            .expect("configured packages are packages");
        let binary_config = self.protocol_config.binary_config(None);
        let mut compiled = modules
            .iter()
            .map(|bytes| CompiledModule::deserialize_with_config(bytes, &binary_config))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| eyre!("substitute modules do not deserialize: {error:?}"))?;
        substitute_package_id(&mut compiled, package).map_err(|error| eyre!("{error:?}"))?;
        let mut module_map = BTreeMap::new();
        for module in &compiled {
            let mut bytes = Vec::new();
            module
                .serialize_with_version(module.version, &mut bytes)
                .expect("deserialized modules serialize");
            module_map.insert(module.name().to_string(), bytes);
        }
        let substitute = MovePackage::new(
            package,
            published_package.version(),
            module_map,
            self.protocol_config.max_move_package_size(),
            published_package.type_origin_table().clone(),
            published_package.linkage_table().clone(),
        )
        .map_err(|error| eyre!("{error:?}"))?;

        // Verified and linked as at publication, on a fresh runtime: the current one caches the
        // published package, which genesis loaded to run its `init`.
        let runtime = runtime(&self.protocol_config);
        let serialized = substitute
            .into_serialized_move_package()
            .map_err(|error| eyre!("{error:?}"))?;
        let genesis = GenesisPackages(&self.genesis);
        let dependencies = TransactionPackageStore::new(&genesis);
        let extensions = Rc::new(RefCell::new(NativeContextExtensions::default()));
        runtime
            .validate_package(
                dependencies,
                *package,
                serialized,
                &mut UnmeteredGasMeter,
                extensions,
            )
            .map_err(|error| eyre!("substitute package fails verification: {error:?}"))?;
        let verifier_config = self.protocol_config.verifier_config(None);
        for module in &compiled {
            sui_verify_module_unmetered(module, &BTreeMap::new(), &verifier_config)
                .map_err(|error| eyre!("substitute module fails Sui's checks: {error:?}"))?;
        }

        let pool = &mut RcPool::new();
        let before = published_package
            .normalize(pool, &binary_config, false)
            .map_err(|error| eyre!("{error:?}"))?;
        let after = normalize_deserialized_modules(pool, compiled.iter(), false);
        if compiled.len() != before.len() || !before.keys().eq(after.keys()) {
            bail!("substitute modules differ from the published ones");
        }
        for (name, module) in &before {
            let substitute = &after[name];
            let compatibility = Compatibility::full_check();
            compatibility
                .check(module, substitute)
                .and_then(|()| compatibility.check(substitute, module))
                .map_err(|error| {
                    eyre!("substitute module {name} changes its interface: {error:?}")
                })?;
        }

        let object = SuiObject::new_from_package(substitute, published.previous_transaction);
        self.substitutes.insert(package, PackageObject::new(object));
        self.runtime = runtime;
        Ok(())
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

    /// Runs `kind` through Sui's adapter.
    fn run<Mode: ExecutionMode, V: StateView>(
        &self,
        store: &SuiView<'_, V>,
        kind: TransactionKind,
        sender: SuiAddress,
        inputs: Vec<ObjectReadResult>,
        digest: TransactionDigest,
    ) -> SuiOutput<Mode> {
        // The sentinel payment Sui reads as unmetered: no gas coin is needed, and nothing is
        // charged against the price or budget.
        let gas_data = GasData {
            payment: vec![(ObjectID::ZERO, SequenceNumber::default(), ObjectDigest::MIN)],
            owner: sender,
            price: 1,
            budget: self.protocol_config.max_tx_gas(),
        };
        // Sui pins the clock's version for every transaction, so `clock::borrow` reads it without
        // the transaction taking it as an input.
        let clock = ObjectId::from_bytes(SUI_CLOCK_OBJECT_ID.into_bytes());
        let system_objects = match store.0.latest_version(&clock) {
            Some(version) => SystemObjectVersions::from_map(BTreeMap::from([(
                SUI_CLOCK_OBJECT_ID,
                ConsensusObjectVersion {
                    initial_shared_version: SUI_CLOCK_OBJECT_SHARED_VERSION,
                    version: SequenceNumber::from_u64(version.as_u64()),
                },
            )])),
            None => SystemObjectVersions::empty(),
        };
        execute_transaction_to_effects::<Mode>(
            store,
            CheckedInputObjects::new_with_checked_transaction_inputs(InputObjects::new(inputs)),
            system_objects,
            &EmptyUnsettledObjectFunds,
            gas_data,
            SuiGasStatus::new_unmetered(&self.protocol_config),
            kind,
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
        )
    }
}

impl ExecutionEngine for MoveEngine {
    fn genesis(&self) -> Vec<Object> {
        self.genesis.iter().map(to_object).collect()
    }

    /// Runs Sui's consensus commit prologue, which sets the clock to the commit's timestamp.
    fn begin_commit<V: StateView>(&self, view: &V, commit: &CommitInfo) -> ExecutionOutput {
        let store = SuiView(view, &self.substitutes);
        let clock = store
            .object(&SUI_CLOCK_OBJECT_ID)
            .expect("genesis creates the clock");
        // Sui's adapter reads only the timestamp.
        let prologue = ConsensusCommitPrologue {
            epoch: EPOCH,
            round: 0,
            commit_timestamp_ms: commit.timestamp_ms,
        };
        // Each prologue moves the clock to a new version, so each gets its own digest; anchor
        // rounds may repeat.
        let name = format!("consensus commit prologue {}", clock.version().value());
        let digest = TransactionDigest::new(Blake2b::<U32>::digest(name).into());
        let input = ObjectReadResult {
            input_object_kind: InputObjectKind::SharedMoveObject {
                id: SUI_CLOCK_OBJECT_ID,
                initial_shared_version: SUI_CLOCK_OBJECT_SHARED_VERSION,
                mutability: SharedObjectMutability::Mutable,
            },
            object: ObjectReadResultKind::Object(clock),
        };
        let kind = TransactionKind::ConsensusCommitPrologue(prologue);
        let output = self.run::<Normal, _>(&store, kind, SuiAddress::ZERO, vec![input], digest);
        assert!(
            output.effects.status().is_ok(),
            "the commit prologue cannot fail"
        );
        ExecutionOutput::success(output.inner_store.written.values().map(to_object).collect())
    }

    fn execute<V: StateView>(&self, view: &V, transaction: &Transaction) -> ExecutionOutput {
        let Some((package, module, function)) = self.functions.get(&transaction.function()) else {
            return ExecutionOutput::aborted(AbortReason::UnknownFunction);
        };
        let Ok(call) = bcs::from_bytes::<MoveCall>(transaction.args()) else {
            return rejected();
        };

        let store = SuiView(view, &self.substitutes);
        let mut builder = ProgrammableTransactionBuilder::new();
        let mut inputs = Vec::with_capacity(transaction.inputs().len());
        let mut objects = Vec::with_capacity(transaction.inputs().len());
        for (id, mode) in transaction.inputs() {
            // Move derives created ids from the transaction digest, so it declares no creations.
            if *mode == AccessMode::WriteOnly {
                return rejected();
            }
            let id = ObjectID::new(*id.as_bytes());
            // Only commits advance the clock: Sui's input checks, which this engine replaces, let
            // transactions read it and nothing more.
            if id == SUI_CLOCK_OBJECT_ID && *mode != AccessMode::ReadOnly {
                return rejected();
            }
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
                    return rejected();
                }
                Owner::AddressOwner(_) | Owner::Immutable if !object.is_package() => {
                    let reference = object.compute_object_reference();
                    let kind = InputObjectKind::ImmOrOwnedMoveObject(reference);
                    (ObjectArg::ImmOrOwnedObject(reference), kind)
                }
                _ => return rejected(),
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
                    None => return rejected(),
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

        let kind = TransactionKind::ProgrammableTransaction(builder.finish());
        let sender = SuiAddress::from(SENDER);
        let digest = sui_digest(transaction);
        let SuiOutput {
            inner_store,
            effects,
            execution_result,
            ..
        } = self.run::<Normal<ExecutionError>, _>(&store, kind, sender, inputs, digest);
        if let SuiStatus::Failure(failure) = effects.status() {
            // Sui keeps the VM's error as the source of its own.
            let vm_status = execution_result.err().and_then(|error| {
                let source = error.source().as_ref()?;
                source.downcast_ref::<VMError>().map(VMError::major_status)
            });
            let failure = MoveFailure {
                status: failure.error.clone(),
                vm_status,
            };
            return ExecutionOutput::aborted(AbortReason::ExecutionFailure(failure.encode()));
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

fn runtime(protocol_config: &ProtocolConfig) -> Arc<MoveRuntime> {
    let natives = all_natives(true, protocol_config);
    let runtime = new_move_runtime(natives, protocol_config).expect("the Move runtime builds");
    Arc::new(runtime)
}

/// Fails a call the engine rejects before Sui runs it.
fn rejected() -> ExecutionOutput {
    ExecutionOutput::aborted(AbortReason::ExecutionFailure(FailureDetail::default()))
}

fn sui_digest(transaction: &Transaction) -> TransactionDigest {
    TransactionDigest::new(*transaction.digest().as_bytes())
}

fn to_object(object: &SuiObject) -> Object {
    let id = ObjectId::from_bytes(object.id().into_bytes());
    let contents = bcs::to_bytes(object).expect("objects serialize");
    Object::new(id, Version::new(object.version().value()), contents)
}
