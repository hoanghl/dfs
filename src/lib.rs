mod components;

// #[cfg(target_os = "android")]
pub mod android {
    use crate::components::{
        configs::Configs,
        entity::{
            client::Client, data::Data, dns::DNS, master::Master,
            node_roles::Role, nodes::Node,
        },
    };
    use android_logger::Config;
    use std::{env, net::Ipv4Addr};

    // use env_logger::Builder;
    use jni::{
        objects::{JClass, JObject, JString, JValue},
        InitArgsBuilder, JNIEnv, JNIVersion, JavaVM,
    };
    use log;
    use serde::{Deserialize, Serialize};
    use serde_json;

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

    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn Java_expo_modules_myrustmodule_MyRustModule_triggerSystem<
        'l,
    >(
        mut env: JNIEnv<'l>,
        _class: JClass,
        arg_str: JString,
        obj: JObject,
    ) {
        // Set up logger
        android_logger::init_once(
            Config::default()
                .with_tag("JNIRust")
                .with_max_level(log::LevelFilter::Debug),
        );

        let new_obj = env.new_global_ref(obj).expect("Not");

        // let mut builder = Builder::from_default_env();
        // builder
        //     .format(move |_buf, record| {
        //         let jvm_args = InitArgsBuilder::new()
        //             // Pass the JNI API version (default is 8)
        //             .version(JNIVersion::V8)
        //             // You can additionally pass any JVM options (standard, like a system property,
        //             // or VM-specific).
        //             // Here we enable some extra JNI checks useful during development
        //             .option("-Xcheck:jni")
        //             .build()
        //             .unwrap();

        //         // Create a new VM
        //         let jvm = JavaVM::new(jvm_args).expect("Cannot create JVM");

        //         // Attach the current thread to call into Java — see extra options in
        //         // "Attaching Native Threads" section.
        //         //
        //         // This method returns the guard that will detach the current thread when dropped,
        //         // also freeing any local references created in it
        //         let mut env =
        //             jvm.attach_current_thread().expect("Cannot create env");

        //         let log_str = &env
        //             .new_string(format!(
        //                 "{} - {}",
        //                 record.level(),
        //                 record.args()
        //             ))
        //             .unwrap();

        //         let _ = env.call_method(
        //             new_obj.clone(),
        //             "setLog",
        //             "(Ljava/lang/String;)V",
        //             &[JValue::Object(log_str)],
        //         );

        //         // writeln!(buf, "{} - {}", record.level(), record.args())
        //         Ok(())
        //     })
        //     .init();

        // env_logger::init();

        // if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "debug");

        // }

        // Parse arguments from Kotlin
        let arg_str_conv = match env.get_string(&arg_str) {
            Ok(x) => {
                String::from(x.to_str().expect("Cannot convert to String"))
            }
            Err(err) => {
                log::error!("Err: {}", err);
                panic!();
            }
        };

        let args: ArgJNI = serde_json::from_str(&arg_str_conv).unwrap();
        // env.new_string(format!("deserialized = {:?}", args))
        //     .unwrap()

        // Convert parsed arguments to config
        let ip_dns = Ipv4Addr::new(
            args.ip_dns[0],
            args.ip_dns[1],
            args.ip_dns[2],
            args.ip_dns[3],
        );

        let mut configs = Configs::initialize(ip_dns, args.port_dns, args.port);
        configs.role = match args.role.parse::<Role>() {
            Ok(role) => role,
            Err(err) => {
                log::error!("Cannot parse 'role' argument: {}", err);
                panic!();
            }
        };

        // Start
        let mut node = Data::new(&configs);
        node.start(&configs.addr_local)

        // match configs.role {
        //     Role::Master => {
        //         Master::new(&configs).start(configs.port);
        //     }
        //     Role::Data => {
        //         let mut node = Data::new(&configs);
        //         // node.start(configs.port);
        //     }
        //     Role::DNS => {
        //         DNS::new(&configs).start(configs.port_dns);
        //     }
        //     Role::Client => {
        //         Client::new(&configs).start(configs.port);
        //     }
        //     _ => panic!("Invalid role argument"),
        // };

        // env.new_string(format!("Done")).unwrap()
    }
}
