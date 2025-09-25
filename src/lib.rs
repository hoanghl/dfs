use std::{env, net::Ipv4Addr};

use env_logger::Builder;
use jni::objects::{JClass, JObject, JString, JValue};
use jni::{InitArgsBuilder, JNIEnv, JNIVersion, JavaVM};
use log;
use serde::{Deserialize, Serialize};
use serde_json;

mod components;
use crate::components::{
    configs::Configs,
    entity::{
        client::Client, data::Data, dns::DNS, master::Master, node_roles::Role,
        nodes::Node,
    },
};

#[repr(C)]
#[derive(Serialize, Deserialize, Debug)]
pub struct ArgJNI {
    #[serde(rename = "ipDns")]
    ip_dns: Vec<u8>,

    #[serde(rename = "portDns")]
    port_dns: u16,

    #[serde(rename = "port")]
    port: u16,

    role: String,
}

// pub fn sendLog(env: &mut JNIEnv) -> Result<_, _> {
//     let class_name = "com/tommy/App";
//     let log_class = env.find_class(class_name)?;
//     let log_class = env.new_global_ref(log_class)?;
//     let log_get_log_meth = env.get_static_method_id(
//         class_name,
//         "getLog",
//         "(Ljava/lang/String;)Lcom/questdb/log/Log;",
//     )?;
//     let log_info_meth = env.get_method_id(class_name, "info", "(IJ)V")?;
//     Ok(Self {
//         logs,
//         log_class,
//         log_get_log_meth,
//         log_info_meth,
//     })
// }

#[unsafe(no_mangle)]
pub extern "C" fn trigger_system(
    mut env: JNIEnv,
    _class: JClass,
    arg_str: JString,
    obj: JObject,
) {
    // Set up logger
    let new_obj = env.new_global_ref(obj).expect("Not");

    let mut builder = Builder::from_default_env();
    builder
        .format(move |_buf, record| {
            let jvm_args = InitArgsBuilder::new()
                // Pass the JNI API version (default is 8)
                .version(JNIVersion::V8)
                // You can additionally pass any JVM options (standard, like a system property,
                // or VM-specific).
                // Here we enable some extra JNI checks useful during development
                .option("-Xcheck:jni")
                .build()
                .unwrap();

            // Create a new VM
            let jvm = JavaVM::new(jvm_args).expect("Cannot create JVM");

            // Attach the current thread to call into Java — see extra options in
            // "Attaching Native Threads" section.
            //
            // This method returns the guard that will detach the current thread when dropped,
            // also freeing any local references created in it
            let mut env =
                jvm.attach_current_thread().expect("Cannot create env");

            let log_str = &env
                .new_string(format!("{} - {}", record.level(), record.args()))
                .unwrap();

            let _ = env.call_method(
                new_obj.clone(),
                "setLog",
                "(Ljava/lang/String;)V",
                &[JValue::Object(log_str)],
            );

            // writeln!(buf, "{} - {}", record.level(), record.args())
            Ok(())
        })
        .init();

    env_logger::init();
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "debug");
    }

    // Parse arguments from Kotlin
    let arg_str_conv = match env.get_string(&arg_str) {
        Ok(x) => String::from(x.to_str().expect("Cannot convert to String")),
        Err(err) => {
            log::error!("{}", err);
            return;
        }
    };

    let args: ArgJNI = serde_json::from_str(&arg_str_conv).unwrap();
    println!("deserialized = {:?}", args);

    // Convert parsed arguments to config
    let ip_dns = Ipv4Addr::new(
        args.ip_dns[0],
        args.ip_dns[1],
        args.ip_dns[2],
        args.ip_dns[3],
    );
    let port_dns = args.port_dns;

    let mut configs = Configs::initialize(ip_dns, port_dns);
    configs.role = match args.role.parse::<Role>() {
        Ok(role) => role,
        Err(err) => {
            log::error!("Cannot parse 'role' argument: {}", err);
            return;
        }
    };
    configs.port = args.port;

    // Start
    match configs.role {
        Role::Master => {
            Master::new(&configs).start(configs.port);
        }
        Role::Data => {
            Data::new(&configs).start(configs.port);
        }
        Role::DNS => {
            DNS::new(&configs).start(configs.port_dns);
        }
        Role::Client => {
            Client::new(&configs).start(configs.port);
        }
        _ => panic!("Invalid role argument"),
    };
}

#[cfg(target_os = "android")]
pub mod android {
    use crate::trigger_system;
    use jni::objects::{JClass, JObject, JString};
    use jni::JNIEnv;

    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn Java_expo_modules_myrustmodule_MyRustModule_triggerSystem(
        env: JNIEnv,
        class: JClass,
        arg_str: JString,
        obj: JObject,
    ) {
        // trigger_system(env, class, arg_str, obj)
    }
}
