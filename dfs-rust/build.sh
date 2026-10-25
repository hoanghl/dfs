# cargo clean
rm -r jniLibs

cargo ndk -t arm64-v8a --platform 35 -o ./jniLibs build --release
#   -t armeabi-v7a

mv ./jniLibs/arm64-v8a/libdfs.so /Users/hoangle/Projects/tests/expo-rust-demo/modules/my-rust-module/android/src/main/jniLibs/arm64-v8a/libdfs.so
# mv ./jniLibs/armeabi-v7a/libdfs.so /Users/hoangle/Projects/tests/expo-rust-demo/modules/my-rust-module/android/src/main/jniLibs/armeabi-v7a/libdfs.so