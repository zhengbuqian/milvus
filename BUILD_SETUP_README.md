# Milvus Build Environment Setup

This repository contains scripts to automatically set up all prerequisites needed to build Milvus from source code. After running the setup script successfully, you should be able to run `make` or `make build-cpp-with-unittest` without any errors.

## Quick Start

### On Host Systems (regular user)
1. **Run the setup script:**
   ```bash
   ./setup_milvus_build_environment.sh
   ```

2. **Reload your shell environment:**
   ```bash
   source ~/.bashrc
   ```

3. **Test the setup:**
   ```bash
   ./test_milvus_build.sh
   ```

4. **Build Milvus:**
   ```bash
   make
   # or for unit tests
   make build-cpp-with-unittest
   ```

### In Docker Containers (as root)
1. **Start a container:**
   ```bash
   docker run -it ubuntu:latest bash
   # You're now root in the container
   ```

2. **Run the setup script:**
   ```bash
   ./setup_milvus_build_environment.sh
   # Works perfectly as root - no sudo needed!
   ```

3. **Test and build:**
   ```bash
   ./test_milvus_build.sh
   make
   ```

## What the Setup Script Does

The `setup_milvus_build_environment.sh` script automatically:

### System Dependencies
- Installs essential build tools (gcc, g++, make, cmake, git, etc.)
- Sets up GCC-12 as the default compiler (required by Conan)
- Installs development libraries (SSL, zlib, OpenBLAS, etc.)
- Installs Python development tools

### Language Runtimes
- **Go**: Installs Go 1.24.4 (meets requirement >= 1.21)
- **CMake**: Installs CMake 3.26.5 (meets requirement >= 3.26)
- **Rust**: Installs Rust 1.89 with Cargo
- **Python**: Sets up virtual environment with Conan 1.64.1

### Environment Configuration
- Creates a Python virtual environment at `~/.milvus-build-env`
- Configures shell environment variables in `~/.bashrc` and `~/.profile`
- Sets up Conan with proper GCC-12 configuration
- Configures Rust toolchain

## Supported Operating Systems

### Linux (Ubuntu/Debian)
- Ubuntu 20.04 or later (recommended)
- Debian-based distributions with `apt` package manager

### Linux (CentOS/RHEL)
- CentOS 7/8 or RHEL 7/8
- Uses Software Collections (SCL) for modern toolchain

### macOS
- macOS Big Sur 11.5 or later (x86_64)
- macOS Monterey 12.0.1 or later (Apple Silicon)
- Requires Homebrew to be pre-installed

## System Requirements

### Hardware
- 8GB of RAM (minimum)
- 50GB of free disk space
- Multi-core CPU (recommended for faster builds)

### Software Prerequisites
- **Linux**: 
  - As regular user: `sudo` privileges for package installation
  - As root user (e.g., in containers): No additional requirements
- **macOS**: Xcode command line tools, Homebrew

## Files Included

- `setup_milvus_build_environment.sh` - Main setup script
- `test_milvus_build.sh` - Environment validation script
- `BUILD_SETUP_README.md` - This documentation file

## Troubleshooting

### Common Issues

1. **Permission Denied**
   ```bash
   chmod +x setup_milvus_build_environment.sh
   chmod +x test_milvus_build.sh
   ```

2. **Running in Docker Containers**
   - ✅ **Works as root**: The script automatically detects root user and runs commands directly
   - ✅ **No sudo needed**: Perfect for minimal container images
   - ✅ **Container-friendly**: Handles environments where sudo isn't installed

3. **Running on Host Systems**
   - ✅ **Works as regular user**: Automatically uses sudo for system operations
   - ✅ **Works as root**: Also supported if you prefer to run as root

4. **Conan Profile Issues**
   ```bash
   rm -rf ~/.conan
   # Re-run the setup script
   ```

5. **Environment Variables Not Set**
   ```bash
   source ~/.bashrc
   # or restart your terminal
   ```

6. **GCC Version Issues**
   - The script automatically installs and configures GCC-12
   - Conan requires a supported GCC version (12 is recommended)

### Verification Steps

Run the test script to verify your environment:
```bash
./test_milvus_build.sh
```

This will check:
- All required tools are installed
- Versions meet minimum requirements
- Conan profile is correctly configured
- Build process can start successfully

### Build Process

After successful setup, you can build Milvus:

```bash
# Full build
make

# Build with unit tests
make build-cpp-with-unittest

# Build only C++ components
make build-cpp

# Clean build
make clean && make
```

### Environment Details

The setup creates:
- Virtual environment: `~/.milvus-build-env/`
- Conan cache: `~/.conan/`
- Build output: `./bin/` and `./lib/`

### Dependencies Installed

**System packages (Ubuntu example):**
- Build tools: gcc-12, g++-12, make, cmake, git
- Libraries: libssl-dev, zlib1g-dev, libopenblas-dev
- Development tools: clang-format, clang-tidy, ccache

**Language runtimes:**
- Go 1.24.4
- Rust 1.89
- Python 3.x with Conan 1.64.1

## Advanced Usage

### Custom Installation Paths

The script uses standard system paths. To customize:
- Go: `/usr/local/go`
- Python venv: `~/.milvus-build-env`
- Conan cache: `~/.conan`

### Manual Environment Setup

If you prefer manual setup, ensure:
1. Go >= 1.21
2. CMake >= 3.26
3. GCC 12 (for Conan compatibility)
4. Rust 1.89
5. Python with Conan 1.64.1
6. All system development libraries

### Building with Docker

For containerized builds, see the official Milvus documentation:
- `build/README.md` - Docker build instructions
- `build/builder.sh` - Containerized build script

## Support

For issues related to:
- **Setup script**: Check this README and run the test script
- **Milvus build**: See official Milvus documentation
- **System-specific issues**: Check OS-specific package managers

## Success Criteria

After running the setup script, these commands should work without errors:
```bash
make                    # Full Milvus build
make build-cpp-with-unittest  # Build with unit tests
```

The setup is considered successful when:
1. All tools are installed with correct versions
2. Conan profile is configured with GCC-12
3. Build process starts without dependency errors
4. Test script passes all checks

---

**Note**: The initial build may take 30-60 minutes as it downloads and compiles third-party dependencies. Subsequent builds will be much faster due to caching.