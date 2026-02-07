// SPDX-License-Identifier: MPL-2.0
// Copyright ijl (2018-2026)

use crate::ffi::{
    PyBoolRef, PyDictRef, PyFloatRef, PyFragmentRef, PyIntRef, PyListRef, PyStrRef,
    PyStrSubclassRef, PyUuidRef,
};
use crate::serialize::error::SerializeError;
use crate::serialize::obtype::{ObType, pyobject_to_obtype};
use crate::serialize::per_type::{
    BoolSerializer, DataclassGenericSerializer, Date, DateTime, DefaultSerializer,
    DictGenericSerializer, EnumSerializer, FloatSerializer, FragmentSerializer, IntSerializer,
    NoneSerializer, NumpyScalar, NumpySerializer, StrSerializer, StrSubclassSerializer, Time, UUID,
};
use crate::serialize::serializer::PyObjectSerializer;
use crate::serialize::state::SerializerState;
use crate::typeref::TUPLE_TYPE;
use crate::util::isize_to_usize;

use core::ptr::NonNull;
use serde::ser::{Serialize, SerializeSeq, Serializer};

pub(crate) struct ZeroListSerializer;

impl ZeroListSerializer {
    pub const fn new() -> Self {
        Self {}
    }
}

impl Serialize for ZeroListSerializer {
    #[inline(always)]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(b"[]")
    }
}

pub(crate) struct ListTupleSerializer {
    #[cfg(not(Py_GIL_DISABLED))]
    data_ptr: *const *mut crate::ffi::PyObject,
    #[cfg(Py_GIL_DISABLED)]
    items: Box<[NonNull<crate::ffi::PyObject>]>,
    state: SerializerState,
    default: Option<NonNull<crate::ffi::PyObject>>,
    len: usize,
}

impl ListTupleSerializer {
    #[cfg(Py_GIL_DISABLED)]
    #[inline]
    unsafe fn from_list_snapshot(ob: PyListRef) -> Box<[NonNull<crate::ffi::PyObject>]> {
        unsafe {
            let list_ptr = ob.as_ptr();
            let mut cs = core::mem::MaybeUninit::<crate::ffi::PyCriticalSection>::uninit();
            crate::ffi::PyCriticalSection_Begin(cs.as_mut_ptr(), list_ptr);

            let len = isize_to_usize(ffi!(Py_SIZE(list_ptr)));
            let data_ptr = (*list_ptr.cast::<crate::ffi::PyListObject>()).ob_item;

            let mut items: Vec<NonNull<crate::ffi::PyObject>> = Vec::with_capacity(len);
            for idx in 0..len {
                let value = *data_ptr.add(idx);
                debug_assert!(!value.is_null());
                ffi!(Py_INCREF(value));
                items.push(nonnull!(value));
            }

            crate::ffi::PyCriticalSection_End(cs.as_mut_ptr());
            items.into_boxed_slice()
        }
    }

    #[cfg(Py_GIL_DISABLED)]
    #[inline]
    unsafe fn from_tuple_snapshot(ptr: *mut crate::ffi::PyObject) -> Box<[NonNull<crate::ffi::PyObject>]> {
        unsafe {
            let data_ptr = (*ptr.cast::<crate::ffi::PyTupleObject>()).ob_item.as_ptr();
            let len = isize_to_usize(ffi!(Py_SIZE(ptr)));

            let mut items: Vec<NonNull<crate::ffi::PyObject>> = Vec::with_capacity(len);
            for idx in 0..len {
                let value = *data_ptr.add(idx);
                debug_assert!(!value.is_null());
                ffi!(Py_INCREF(value));
                items.push(nonnull!(value));
            }
            items.into_boxed_slice()
        }
    }

    pub fn from_list(
        ob: PyListRef,
        state: SerializerState,
        default: Option<NonNull<crate::ffi::PyObject>>,
    ) -> Self {
        #[cfg(not(Py_GIL_DISABLED))]
        {
            Self {
                data_ptr: ob.data_ptr(),
                len: ob.len(),
                state: state.copy_for_recursive_call(),
                default: default,
            }
        }
        #[cfg(Py_GIL_DISABLED)]
        {
            let items = unsafe { Self::from_list_snapshot(ob) };
            let len = items.len();
            Self {
                items,
                len,
                state: state.copy_for_recursive_call(),
                default: default,
            }
        }
    }

    pub fn from_tuple(
        ptr: *mut crate::ffi::PyObject,
        state: SerializerState,
        default: Option<NonNull<crate::ffi::PyObject>>,
    ) -> Self {
        debug_assert!(
            is_type!(ob_type!(ptr), TUPLE_TYPE)
                || is_subclass_by_flag!(tp_flags!(ob_type!(ptr)), Py_TPFLAGS_TUPLE_SUBCLASS)
        );
        #[cfg(not(Py_GIL_DISABLED))]
        {
            let data_ptr = unsafe { (*ptr.cast::<crate::ffi::PyTupleObject>()).ob_item.as_ptr() };
            let len = isize_to_usize(ffi!(Py_SIZE(ptr)));
            Self {
                data_ptr: data_ptr,
                len: len,
                state: state.copy_for_recursive_call(),
                default: default,
            }
        }
        #[cfg(Py_GIL_DISABLED)]
        {
            let items = unsafe { Self::from_tuple_snapshot(ptr) };
            let len = items.len();
            Self {
                items,
                len,
                state: state.copy_for_recursive_call(),
                default: default,
            }
        }
    }
}

#[cfg(Py_GIL_DISABLED)]
impl Drop for ListTupleSerializer {
    fn drop(&mut self) {
        for ptr in self.items.iter() {
            unsafe {
                ffi!(Py_DECREF(ptr.as_ptr()));
            }
        }
    }
}

impl Serialize for ListTupleSerializer {
    #[inline(never)]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if self.state.recursion_limit() {
            cold_path!();
            err!(SerializeError::RecursionLimit)
        }
        if self.len == 0 {
            cold_path!();
            return ZeroListSerializer::new().serialize(serializer);
        }
        let mut seq = serializer.serialize_seq(None).unwrap();
        #[cfg(not(Py_GIL_DISABLED))]
        for idx in 0..self.len {
            let value = unsafe { *((self.data_ptr).add(idx)) };
            match pyobject_to_obtype(value, self.state.opts()) {
                ObType::Str => {
                    seq.serialize_element(&StrSerializer::new(unsafe {
                        PyStrRef::from_ptr_unchecked(value)
                    }))?;
                }
                ObType::StrSubclass => {
                    seq.serialize_element(&StrSubclassSerializer::new(unsafe {
                        PyStrSubclassRef::from_ptr_unchecked(value)
                    }))?;
                }
                ObType::Int => {
                    seq.serialize_element(&IntSerializer::new(
                        unsafe { PyIntRef::from_ptr_unchecked(value) },
                        self.state.opts(),
                    ))?;
                }
                ObType::None => {
                    seq.serialize_element(&NoneSerializer::new()).unwrap();
                }
                ObType::Float => {
                    seq.serialize_element(&FloatSerializer::new(unsafe {
                        PyFloatRef::from_ptr_unchecked(value)
                    }))?;
                }
                ObType::Bool => {
                    seq.serialize_element(&BoolSerializer::new(unsafe {
                        PyBoolRef::from_ptr_unchecked(value)
                    }))
                    .unwrap();
                }
                ObType::Datetime => {
                    seq.serialize_element(&DateTime::new(value, self.state.opts()))?;
                }
                ObType::Date => {
                    seq.serialize_element(&Date::new(value))?;
                }
                ObType::Time => {
                    seq.serialize_element(&Time::new(value, self.state.opts()))?;
                }
                ObType::Uuid => {
                    seq.serialize_element(&UUID::new(unsafe {
                        PyUuidRef::from_ptr_unchecked(value)
                    }))
                    .unwrap();
                }
                ObType::Dict => {
                    let pyvalue = DictGenericSerializer::new(
                        unsafe { PyDictRef::from_ptr_unchecked(value) },
                        self.state,
                        self.default,
                    );
                    seq.serialize_element(&pyvalue)?;
                }
                ObType::List => {
                    if ffi!(Py_SIZE(value)) == 0 {
                        seq.serialize_element(&ZeroListSerializer::new()).unwrap();
                    } else {
                        let pyvalue = ListTupleSerializer::from_list(
                            unsafe { PyListRef::from_ptr_unchecked(value) },
                            self.state,
                            self.default,
                        );
                        seq.serialize_element(&pyvalue)?;
                    }
                }
                ObType::Tuple => {
                    if ffi!(Py_SIZE(value)) == 0 {
                        seq.serialize_element(&ZeroListSerializer::new()).unwrap();
                    } else {
                        let pyvalue =
                            ListTupleSerializer::from_tuple(value, self.state, self.default);
                        seq.serialize_element(&pyvalue)?;
                    }
                }
                ObType::Dataclass => {
                    seq.serialize_element(&DataclassGenericSerializer::new(
                        &PyObjectSerializer::new(value, self.state, self.default),
                    ))?;
                }
                ObType::Enum => {
                    seq.serialize_element(&EnumSerializer::new(&PyObjectSerializer::new(
                        value,
                        self.state,
                        self.default,
                    )))?;
                }
                ObType::NumpyArray => {
                    seq.serialize_element(&NumpySerializer::new(&PyObjectSerializer::new(
                        value,
                        self.state,
                        self.default,
                    )))?;
                }
                ObType::NumpyScalar => {
                    seq.serialize_element(&NumpyScalar::new(value, self.state.opts()))?;
                }
                ObType::Fragment => {
                    seq.serialize_element(&FragmentSerializer::new(unsafe {
                        PyFragmentRef::from_ptr_unchecked(value)
                    }))?;
                }
                ObType::Unknown => {
                    seq.serialize_element(&DefaultSerializer::new(&PyObjectSerializer::new(
                        value,
                        self.state,
                        self.default,
                    )))?;
                }
            }
        }
        #[cfg(Py_GIL_DISABLED)]
        for ptr in self.items.iter() {
            let value = ptr.as_ptr();
            match pyobject_to_obtype(value, self.state.opts()) {
                ObType::Str => {
                    seq.serialize_element(&StrSerializer::new(unsafe {
                        PyStrRef::from_ptr_unchecked(value)
                    }))?;
                }
                ObType::StrSubclass => {
                    seq.serialize_element(&StrSubclassSerializer::new(unsafe {
                        PyStrSubclassRef::from_ptr_unchecked(value)
                    }))?;
                }
                ObType::Int => {
                    seq.serialize_element(&IntSerializer::new(
                        unsafe { PyIntRef::from_ptr_unchecked(value) },
                        self.state.opts(),
                    ))?;
                }
                ObType::None => {
                    seq.serialize_element(&NoneSerializer::new()).unwrap();
                }
                ObType::Float => {
                    seq.serialize_element(&FloatSerializer::new(unsafe {
                        PyFloatRef::from_ptr_unchecked(value)
                    }))?;
                }
                ObType::Bool => {
                    seq.serialize_element(&BoolSerializer::new(unsafe {
                        PyBoolRef::from_ptr_unchecked(value)
                    }))
                    .unwrap();
                }
                ObType::Datetime => {
                    seq.serialize_element(&DateTime::new(value, self.state.opts()))?;
                }
                ObType::Date => {
                    seq.serialize_element(&Date::new(value))?;
                }
                ObType::Time => {
                    seq.serialize_element(&Time::new(value, self.state.opts()))?;
                }
                ObType::Uuid => {
                    seq.serialize_element(&UUID::new(unsafe {
                        PyUuidRef::from_ptr_unchecked(value)
                    }))
                    .unwrap();
                }
                ObType::Dict => {
                    let pyvalue = DictGenericSerializer::new(
                        unsafe { PyDictRef::from_ptr_unchecked(value) },
                        self.state,
                        self.default,
                    );
                    seq.serialize_element(&pyvalue)?;
                }
                ObType::List => {
                    let pyvalue = ListTupleSerializer::from_list(
                        unsafe { PyListRef::from_ptr_unchecked(value) },
                        self.state,
                        self.default,
                    );
                    seq.serialize_element(&pyvalue)?;
                }
                ObType::Tuple => {
                    if ffi!(Py_SIZE(value)) == 0 {
                        seq.serialize_element(&ZeroListSerializer::new()).unwrap();
                    } else {
                        let pyvalue = ListTupleSerializer::from_tuple(value, self.state, self.default);
                        seq.serialize_element(&pyvalue)?;
                    }
                }
                ObType::Dataclass => {
                    seq.serialize_element(&DataclassGenericSerializer::new(
                        &PyObjectSerializer::new(value, self.state, self.default),
                    ))?;
                }
                ObType::Enum => {
                    seq.serialize_element(&EnumSerializer::new(&PyObjectSerializer::new(
                        value,
                        self.state,
                        self.default,
                    )))?;
                }
                ObType::NumpyArray => {
                    seq.serialize_element(&NumpySerializer::new(&PyObjectSerializer::new(
                        value,
                        self.state,
                        self.default,
                    )))?;
                }
                ObType::NumpyScalar => {
                    seq.serialize_element(&NumpyScalar::new(value, self.state.opts()))?;
                }
                ObType::Fragment => {
                    seq.serialize_element(&FragmentSerializer::new(unsafe {
                        PyFragmentRef::from_ptr_unchecked(value)
                    }))?;
                }
                ObType::Unknown => {
                    seq.serialize_element(&DefaultSerializer::new(&PyObjectSerializer::new(
                        value,
                        self.state,
                        self.default,
                    )))?;
                }
            }
        }
        seq.end()
    }
}
