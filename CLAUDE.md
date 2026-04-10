## Code Format

Format Terraform code using `terraform fmt`.

Format Java code using `mvn com.spotify.fmt:fmt-maven-plugin:format validate`.

## Tests

Unit tests are in `src/test/java/forest/colver/datatransfer/` (root package) and can be run anytime:
```
mvn test -Dexcludes="**/it/**"
```

Integration tests are in the `it/` sub-package and require credentials. Never run all integration tests. When the user
says it is okay to run integration tests, run only the ones relevant to the changed code:
```
mvn test -Dtest="RelevantIntTestClass"
```

## Git

Do not create branches in this repository.
