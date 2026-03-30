# profilesLambdaFunction

AWS Lambda function repository for packaging and deploying the Profiles ingest Lambda.

## Structure

- `src/` - Lambda source code
- `tests/` - Unit tests
- `package.sh` - Packages Lambda into zip
- `.github/workflows/deploy.yml` - GitHub Actions workflow

## Local package

```bash
./package.sh profiles_ingest.zip