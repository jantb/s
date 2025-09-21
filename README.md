# s
kotlin search for k8

``` shell
./gradlew fatJar
cp build/libs/s-1.0-SNAPSHOT-all.jar ~/bin/search.jar
alias search="java --add-exports=java.desktop/com.apple.eawt=ALL-UNNAMED -jar ~/bin/search.jar"
```
