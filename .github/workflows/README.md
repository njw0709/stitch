# GitHub Actions Workflows

This directory contains automated workflows for the HRS Linkage Tool.

## build-release.yml

Automated multi-platform build and release workflow.

### Trigger

- **Automatic**: Pushes to tags matching `v*.*.*` (e.g., `v0.1.0`, `v1.2.3`)
- **Manual**: Can be triggered via GitHub Actions UI

### Workflow Steps

1. **build-macos-arm**: Build macOS ARM application
   - Runs on `macos-latest` (Apple Silicon)
   - Uses PyInstaller to create `.app` bundle
   - Creates ZIP archive using `ditto` (preserves macOS attributes): `STITCH-macOS-ARM.zip`

2. **build-windows**: Build Windows application
   - Runs on `windows-latest`
   - Uses PyInstaller to create `.exe` application
   - Creates ZIP archive: `STITCH-Windows.zip`

3. **create-release**: Create GitHub Release
   - Depends on successful macOS and Windows builds
   - Creates published GitHub Release with version tag
   - Uploads both platform builds as release assets
   - Generates release notes from commits

### Dependencies

- Python 3.9
- [astral-sh/setup-uv](https://github.com/astral-sh/setup-uv) - For dependency management
- [softprops/action-gh-release](https://github.com/softprops/action-gh-release) - For creating releases

### Environment Variables

- `GITHUB_TOKEN`: Automatically provided by GitHub Actions for API access

### Permissions Required

```yaml
permissions:
  contents: write  # Required for creating releases and pushing to repository
```

### Outputs

For each release, the workflow produces:
- **GitHub Release** with version tag and release notes
- **macOS ARM Build**: `STITCH-macOS-ARM.zip` containing `.app` bundle
- **Windows Build**: `STITCH-Windows.zip` containing `.exe` and dependencies

### Example Usage

```bash
# Create a new release
git tag -a v0.2.0 -m "Release v0.2.0

New features:
- Feature 1
- Feature 2

Bug fixes:
- Fix 1
"
git push origin v0.2.0
```

### Monitoring

View workflow runs at:
`https://github.com/njw0709/linkdata/actions/workflows/build-release.yml`

### Troubleshooting

**macOS or Windows build fails:**
- Check PyInstaller spec file (`gui_app.spec`)
- Ensure all dependencies are listed in `pyproject.toml`
- Check for hidden import issues
- Ensure Python 3.9 is being used
- Check Qt plugins are correctly excluded in `gui_app.spec`

**macOS app fails to open after download:**
- The workflow uses `ditto` instead of `zip` to preserve macOS attributes
- If users still have issues, they can run: `xattr -cr HRSLinkageTool.app`

**Release not created:**
- Verify tag format matches `v*.*.*`
- Check that both build jobs completed successfully
- Verify `GITHUB_TOKEN` has correct permissions

