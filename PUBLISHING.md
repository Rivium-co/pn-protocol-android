# PN Protocol - Maven Central Publishing Guide

## Published Artifact

```
co.rivium:pn-protocol
```

Current version: `0.1.0`

### Usage in projects

```gradle
// build.gradle.kts
implementation("co.rivium:pn-protocol:0.1.0")
```

---

## One-Time Setup (Already Done)

These steps only need to be done once. Documented here for reference.

### 1. Sonatype Central Account

1. Created account at https://central.sonatype.com
2. Verified `co.rivium` namespace via DNS TXT record on Cloudflare
3. Generated user token (username + password) from account settings

### 2. GPG Key

1. Installed GPG: `brew install gnupg`
2. Generated RSA 4096-bit key for `founder@rivium.co` (Key ID: `9D352D44`)
3. Uploaded public key to keyserver: `gpg --keyserver keyserver.ubuntu.com --send-keys 9D352D44`

### 3. Gradle Credentials

File: `~/.gradle/gradle.properties`

```properties
# Maven Central (Sonatype) credentials
mavenCentralUsername=<your-token-username>
mavenCentralPassword=<your-token-password>

# GPG Signing (in-memory key)
signingInMemoryKeyId=9D352D44
signingInMemoryKeyPassword=<your-gpg-passphrase>
signingInMemoryKey=<your-armored-private-key-on-one-line-with-backslash-continuations>
```

> These credentials live in your HOME directory (`~/.gradle/`), NOT in the project.
> Never commit these to git.

### 4. Project Configuration

Plugin: `com.vanniktech.maven.publish` version `0.30.0`

Key files:
- `protocol/android/build.gradle.kts` - root build (AGP + Kotlin versions)
- `protocol/android/pn-protocol/build.gradle.kts` - library + publishing config
- `protocol/android/settings.gradle.kts` - project structure
- `protocol/android/gradle.properties` - Gradle JVM settings

---

## Publishing a New Version

### Step 1: Make your code changes

Edit the source files in:
```
protocol/android/pn-protocol/src/main/java/co/rivium/protocol/
```

### Step 2: Update the version number

Edit `protocol/android/pn-protocol/build.gradle.kts`:

```kotlin
mavenPublishing {
    // Change the version here:
    coordinates("co.rivium", "pn-protocol", "0.2.0")  // <-- new version
    ...
}
```

> Maven Central does NOT allow re-publishing the same version.
> Always increment the version: `0.1.0` -> `0.2.0` -> `0.3.0` -> `1.0.0`

### Step 3: Publish

```bash
cd /Users/alitazik/Projects/RiviumPush/android
./gradlew :pn-protocol:publishAndReleaseToMavenCentral
```

That's it. The command will:
1. Compile the library
2. Generate Javadoc and sources JARs
3. Sign all artifacts with GPG
4. Upload to Maven Central Portal
5. Automatically release (no manual approval needed)

### Step 4: Verify

After a few minutes, check that the new version is available:
- https://central.sonatype.com/artifact/co.rivium/pn-protocol

---

## Version Numbering (Semantic Versioning)

```
MAJOR.MINOR.PATCH
```

- **PATCH** (0.1.0 -> 0.1.1): Bug fixes, no API changes
- **MINOR** (0.1.0 -> 0.2.0): New features, backward compatible
- **MAJOR** (0.x.x -> 1.0.0): Breaking API changes

---

## Troubleshooting

### "401 Unauthorized" on createStagingRepository
Your Sonatype token may have expired. Generate a new one at https://central.sonatype.com and update `~/.gradle/gradle.properties`.

### GPG signing fails
Make sure `gpg` is installed (`brew install gnupg`) and your key hasn't expired:
```bash
gpg --list-keys 9D352D44
```

### "Could not resolve com.vanniktech.maven.publish"
Make sure `gradlePluginPortal()` is in your `settings.gradle.kts` pluginManagement repositories.

### Version already exists
Maven Central rejects duplicate versions. You must bump the version number in `build.gradle.kts` before publishing.

---

## Quick Reference

| What                    | Where                                                    |
|-------------------------|----------------------------------------------------------|
| Source code             | `protocol/android/pn-protocol/src/main/java/co/rivium/protocol/` |
| Publishing config       | `protocol/android/pn-protocol/build.gradle.kts`         |
| Credentials             | `~/.gradle/gradle.properties`                            |
| Publish command (from `android/`) | `./gradlew :pn-protocol:publishAndReleaseToMavenCentral` |
| Maven Central dashboard | https://central.sonatype.com                             |
| GPG Key ID              | `9D352D44`                                               |
| Group ID                | `co.rivium`                                              |
| Artifact ID             | `pn-protocol`                                            |
