#[macro_export]
macro_rules! create_pointer {
    ($pointer_name:ident,$pointertype:ident) => {
        #[derive(Debug, Clone, Copy, abomonation_derive::Abomonation)]
        pub struct $pointer_name(usize);

        impl $pointer_name {
            #[allow(clippy::ptr_arg)]
            pub fn new(cube: &$pointertype) -> Self {
                Self(cube as *const $pointertype as usize)
            }
        }

        impl std::ops::Deref for $pointer_name {
            type Target = $pointertype;

            fn deref(&self) -> &Self::Target {
                let p = self.0 as *const $pointertype;
                unsafe { &*p }
            }
        }
    };
}

#[macro_export]
macro_rules! create_generic_pointer {
    ($pointer_name:ident,$pointertype:ident) => {
        #[derive(Debug, Clone, Copy, abomonation_derive::Abomonation)]
        pub struct $pointer_name<T>(usize, std::marker::PhantomData<T>);

        impl<T> $pointer_name<T> {
            pub fn new(cube: &$pointertype<T>) -> Self {
                Self(cube as *const $pointertype<T> as usize, std::marker::PhantomData)
            }
        }

        impl<T> std::ops::Deref for $pointer_name<T> {
            type Target = $pointertype<T>;

            fn deref(&self) -> &Self::Target {
                let p = self.0 as *const $pointertype<T>;
                unsafe { &*p }
            }
        }
    };
}

#[macro_export]
macro_rules! create_generic_pointer_with_bounds {
    ($pointer_name:ident,$pointertype:ident,$bounds:ident) => {
        #[derive(Debug, Clone, Copy, abomonation_derive::Abomonation)]
        pub struct $pointer_name<T: $bounds>(usize, std::marker::PhantomData<T>);

        impl<T: $bounds> $pointer_name<T> {
            pub fn new(cube: &$pointertype<T>) -> Self {
                Self(cube as *const $pointertype<T> as usize, std::marker::PhantomData)
            }
        }

        impl<T: $bounds> std::ops::Deref for $pointer_name<T> {
            type Target = $pointertype<T>;

            fn deref(&self) -> &Self::Target {
                let p = self.0 as *const $pointertype<T>;
                unsafe { &*p }
            }
        }
    };
}

#[macro_export]
macro_rules! create_generic_mut_pointer_with_bounds {
    ($pointer_name:ident,$pointertype:ident,$bounds:ident) => {
        #[derive(Debug, Clone, Copy, abomonation_derive::Abomonation)]
        pub struct $pointer_name<T: $bounds>(usize, std::marker::PhantomData<T>);

        impl<T: $bounds> $pointer_name<T> {
            pub fn new(cube: &mut $pointertype<T>) -> Self {
                Self(cube as *mut $pointertype<T> as usize, std::marker::PhantomData)
            }
        }

        impl<T: $bounds> std::ops::Deref for $pointer_name<T> {
            type Target = $pointertype<T>;

            fn deref(&self) -> &Self::Target {
                let p = self.0 as *const $pointertype<T>;
                unsafe { &*p }
            }
        }

        impl<T: $bounds> std::ops::DerefMut for $pointer_name<T> {
            fn deref_mut(&mut self) -> &mut Self::Target {
                let p = self.0 as *mut $pointertype<T>;
                unsafe { &mut *p }
            }
        }
    };
}
