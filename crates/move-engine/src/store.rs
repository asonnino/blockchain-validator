// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Sui's storage traits over a [`StateView`], whose objects hold BCS-encoded Sui objects.

use execution::{object::ObjectId, store::StateView};
use sui_types::{
    base_types::{ObjectID, ObjectRef, SequenceNumber, SuiAddress, VersionNumber},
    committee::EpochId,
    error::SuiResult,
    object::{Object, Owner},
    storage::{BackingPackageStore, ObjectStore, PackageObject, ParentSync, RuntimeObjectResolver},
};

/// Executions are sequential, so the view's latest version is the only one Sui can ask for.
pub(crate) struct SuiView<'v, V>(pub(crate) &'v V);

impl<V: StateView> SuiView<'_, V> {
    pub(crate) fn object(&self, id: &ObjectID) -> Option<Object> {
        let object = self.0.latest(&ObjectId::from_bytes(id.into_bytes()))?;
        Some(bcs::from_bytes(object.contents()).expect("stored objects are Sui objects"))
    }
}

impl<V: StateView> ObjectStore for SuiView<'_, V> {
    fn get_object(&self, id: &ObjectID) -> Option<Object> {
        self.object(id)
    }

    fn get_object_by_key(&self, id: &ObjectID, version: VersionNumber) -> Option<Object> {
        self.object(id).filter(|object| object.version() == version)
    }
}

impl<V: StateView> BackingPackageStore for SuiView<'_, V> {
    fn get_package_object(&self, id: &ObjectID) -> SuiResult<Option<PackageObject>> {
        Ok(self
            .object(id)
            .filter(|object| object.is_package())
            .map(PackageObject::new))
    }
}

impl<V: StateView> RuntimeObjectResolver for SuiView<'_, V> {
    fn read_child_object(
        &self,
        parent: &ObjectID,
        child: &ObjectID,
        child_version_upper_bound: SequenceNumber,
    ) -> SuiResult<Option<Object>> {
        let owner = Owner::ObjectOwner(SuiAddress::from(*parent));
        Ok(self.object(child).filter(|object| {
            object.owner == owner && object.version() <= child_version_upper_bound
        }))
    }

    fn get_object_received_at_version(
        &self,
        owner: &ObjectID,
        receiving_object_id: &ObjectID,
        receive_object_at_version: SequenceNumber,
        _epoch_id: EpochId,
    ) -> SuiResult<Option<Object>> {
        let owner = Owner::AddressOwner(SuiAddress::from(*owner));
        Ok(self.object(receiving_object_id).filter(|object| {
            object.owner == owner && object.version() == receive_object_at_version
        }))
    }
}

impl<V: StateView> ParentSync for SuiView<'_, V> {
    fn get_latest_parent_entry_ref_deprecated(&self, id: ObjectID) -> Option<ObjectRef> {
        self.object(&id)
            .map(|object| object.compute_object_reference())
    }
}
