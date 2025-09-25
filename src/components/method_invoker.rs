pub(crate) struct StaticMethodInvoker {
    class: GlobalRef,
    method_id: JStaticMethodID,
    ret: ReturnType,
}

impl StaticMethodInvoker {
    fn try_new(
        env: &mut JNIEnv,
        class_name: &str,
        method_name: &str,
        sig: &str,
        ret: ReturnType,
    ) -> Result<Self> {
        let class = env.find_class(class_name)?;
        let class = env.new_global_ref(class)?;
        let method_id =
            env.get_static_method_id(class_name, method_name, sig)?;
        Ok(Self {
            class,
            method_id,
            ret,
        })
    }

    pub(crate) unsafe fn invoke<'local>(
        &self,
        env: &mut JNIEnv<'local>,
        args: &[jvalue],
    ) -> Result<JValueOwned<'local>> {
        env.call_static_method_unchecked(
            &self.class,
            self.method_id,
            self.ret.clone(),
            args,
        )
    }
}
