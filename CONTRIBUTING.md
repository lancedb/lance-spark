# Contributing to Spark Lance Connector

The Spark Lance connector codebase is at [lancedb/lance-spark](https://github.com/lancedb/lance-spark).

## Build Commands

This connector is built using Maven. You can run the following make commands:

```shell
# Build all
make build

# Clean all
make clean

# Build Spark 3.5 Scala 2.12
make build-35-212

# Clean build of Spark 3.5 Scala 2.12
make clean-35-212

# Build the runtime bundle of Spark 3.5 Scala 2.12
make bundle-35-212
```

## Styling Guide

We use checkstyle and spotless to lint the code.

All the `make build` commands automatically perform `spotless:apply` to the code.

To verify style, run:

```shell
make lint
```

## Documentation

### Setup

The documentation website is built using [mkdocs-material](https://pypi.org/project/mkdocs-material).
The build system require [uv](https://docs.astral.sh/uv/).

Start the server with:

```shell
make serve-docs
```

### Understanding the Build Process

The contents in `lance-spark/docs` are for the ease of contributors to edit and preview.
After code merge, the contents are added to the 
[main Lance documentation](https://github.com/lancedb/lance/tree/main/docs) 
during the Lance doc CI build time, and is presented in the Lance website under 
[Apache Spark integration](https://lancedb.github.io/lance/integrations/spark).

The CONTRIBUTING.md document is auto-built to the [Lance Contributing Guide](https://lancedb.github.io/lance/community/contributing/)

## Release Process

This section describes the CI/CD workflows for automated version management, releases, and publishing.

### Workflows

#### Auto Bump Version (`auto-bump.yml`)

**Purpose:** Creates PR to bump version when changes are detected.

**Triggers:**
- Manual trigger via GitHub Actions UI

**Features:**
- Detects unreleased changes since last tag
- Supports automatic or manual bump type selection:
  - `auto`: Analyzes commit messages to determine bump type
  - `patch`, `minor`, `major`: Manual override
- Automatic detection rules:
  - `BREAKING CHANGE` or `!:` → major bump
  - `feat:` or `feature:` → minor bump
  - Other changes → patch bump
- Creates a pull request with version changes

#### Create Release (`release.yml`)

**Purpose:** Creates a new release with version bumping and tagging.

**Triggers:**
- Manual trigger via GitHub Actions UI

**Inputs:**
- `release_type`: major, minor, or patch
- `release_channel`: stable or preview
- `dry_run`: simulate without pushing changes
- `draft_release`: create as draft on GitHub

**Features:**
- Updates version in all pom.xml files (stable releases)
- Creates git tag (semantic versioning)
- Generates release notes automatically
- Supports preview/beta releases
- Publishing to Maven Central is triggered automatically when a stable release is published (not draft)

#### Publish Spark Packages (`publish.yml`)

**Purpose:** Publishes artifacts to Maven Central.

**Triggers:**
- Automatically when a GitHub release is published (not draft)
- Manual trigger via workflow dispatch
- Pull request changes to the workflow file (dry run)

**Features:**
- Builds and publishes all modules to Maven Central
- Signs artifacts with GPG
- Supports dry run mode for testing
- Uses Sonatype OSSRH for distribution

### Required Secrets

Configure these secrets in your GitHub repository settings:

- `GITHUB_TOKEN`: Automatically provided by GitHub Actions
- `SONATYPE_USER`: Sonatype OSSRH username
- `SONATYPE_TOKEN`: Sonatype OSSRH token
- `GPG_PRIVATE_KEY`: GPG private key for signing artifacts
- `GPG_PASSPHRASE`: Passphrase for GPG key

### Creating a Release

1. **Version Bump (Optional but Recommended)**
   - Go to Actions → "Auto Bump Version"
   - Select bump type or use "auto" for automatic detection
   - Review and merge the created PR

2. **Create Release**
   - Go to Actions → "Create Release"
   - Select parameters:
     - Release type (major/minor/patch)
     - Release channel (stable/preview)
     - Dry run (test without pushing)
     - Draft release (create as draft)
   - Run workflow

3. **Publishing**
   - For stable releases: Maven Central publishing is triggered automatically when the release is published (not draft)
   - For draft releases: Edit and publish the release on GitHub to trigger publishing
   - For preview releases: Not published to Maven Central

### Manual Version Bump

Run locally:
```bash
python ci/bump_version.py --version 1.2.3
```

### Testing Workflows

Use dry run mode to test without making changes:
1. Go to Actions → "Create Release"
2. Set `dry_run` to `true`
3. Review the summary output

### Version Scheme

- **Stable releases:** `X.Y.Z` (e.g., 1.2.3)
- **Preview releases:** `X.Y.Z-beta.N` (e.g., 1.2.3-beta.1)

### Commit Message Convention

For automatic bump type detection:
- `feat:` or `feature:` → minor version bump
- `fix:` → patch version bump
- `BREAKING CHANGE` or `!:` → major version bump
- `chore:`, `docs:`, `test:` → patch version bump

### Troubleshooting

#### Release workflow fails
- Check that all required secrets are configured
- Verify GPG key is valid and not expired
- Ensure Sonatype credentials are correct

#### Auto-bump doesn't create PR
- Check if there are commits since last tag
- Verify GitHub Actions has write permissions
- Check workflow logs for errors

#### Publish workflow doesn't trigger
- Ensure the release is published (not draft)
- Check that it's a stable release (not preview/beta)
- Verify the publish.yml workflow is enabled

#### Maven publish fails
- Verify artifacts build successfully locally
- Check Sonatype OSSRH status
- Ensure GPG signing works locally

### Support Scripts

#### `ci/bump_version.py`
Updates version in all pom.xml files.

#### `ci/calculate_version.py`
Calculates next version based on release type.

#### `ci/generate_release_notes.py`
Generates release notes from git history and PR information. 