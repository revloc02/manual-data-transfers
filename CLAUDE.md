## Code Format

Format Terraform code using `terraform fmt`.

Format Java code using `mvn com.spotify.fmt:fmt-maven-plugin:format validate`.

## Integration Tests

The integration tests are not meant to be generally run to verify code changes. They require specific credentials to
make connections to various services and those credentials are not always available.

## Git

Do not create branches in this repository.
