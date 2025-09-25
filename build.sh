cargo clean
rm -r jniLibs

cargo ndk -t armeabi-v7a -t arm64-v8a --platform 35 -o ./jniLibs build --release
#  

rm -r /Users/hoangle/Projects/tests/expo-rust-demo/modules/my-rust-module/android/src/main/jniLibs

mv jniLibs /Users/hoangle/Projects/tests/expo-rust-demo/modules/my-rust-module/android/src/main/.