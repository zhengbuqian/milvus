#!/bin/bash

# Milvus Build Environment Setup Script
# This script sets up all prerequisites needed to build Milvus from source
# Success criteria: After running this script, `make` or `make build-cpp-with-unittest` should work without errors

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running as root
check_root() {
    if [[ $EUID -eq 0 ]]; then
        log_error "This script should not be run as root. Please run as a regular user with sudo privileges."
        exit 1
    fi
}

# Check if sudo is available
check_sudo() {
    if ! sudo -n true 2>/dev/null; then
        log_error "This script requires sudo privileges. Please run: sudo -v"
        exit 1
    fi
}

# Detect OS
detect_os() {
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        if command -v apt-get &> /dev/null; then
            OS="ubuntu"
            log_info "Detected Ubuntu/Debian system"
        elif command -v yum &> /dev/null; then
            OS="centos"
            log_info "Detected CentOS/RHEL system"
        else
            log_error "Unsupported Linux distribution"
            exit 1
        fi
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        OS="macos"
        log_info "Detected macOS system"
    else
        log_error "Unsupported operating system: $OSTYPE"
        exit 1
    fi
}

# Install system dependencies for Ubuntu/Debian
install_ubuntu_deps() {
    log_info "Updating package lists..."
    sudo apt update

    log_info "Installing system dependencies..."
    sudo apt install -y \
        wget curl ca-certificates gnupg2 \
        gcc-12 g++-12 gfortran git make ccache \
        libssl-dev zlib1g-dev zip unzip \
        clang-format clang-tidy lcov libtool m4 autoconf automake \
        python3 python3-pip python3-venv python3-dev \
        pkg-config uuid-dev libaio-dev libopenblas-dev libgoogle-perftools-dev \
        build-essential

    # Set GCC-12 as default
    log_info "Setting up GCC-12 as default compiler..."
    sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-12 60 --slave /usr/bin/g++ g++ /usr/bin/g++-12
    if command -v gcc-14 &> /dev/null; then
        sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-14 40 --slave /usr/bin/g++ g++ /usr/bin/g++-14
    fi
}

# Install system dependencies for CentOS/RHEL
install_centos_deps() {
    log_info "Installing EPEL and SCL repositories..."
    sudo yum install -y epel-release centos-release-scl-rh

    log_info "Installing system dependencies..."
    sudo yum install -y \
        wget curl which git make automake python3-devel \
        devtoolset-11-gcc devtoolset-11-gcc-c++ devtoolset-11-gcc-gfortran devtoolset-11-libatomic-devel \
        llvm-toolset-11.0-clang llvm-toolset-11.0-clang-tools-extra openblas-devel \
        libaio libuuid-devel zip unzip ccache lcov libtool m4 autoconf automake

    # Set up SCL environment
    echo "source scl_source enable devtoolset-11" | sudo tee -a /etc/profile.d/devtoolset-11.sh
    echo "source scl_source enable llvm-toolset-11.0" | sudo tee -a /etc/profile.d/llvm-toolset-11.sh
    echo "export CLANG_TOOLS_PATH=/opt/rh/llvm-toolset-11.0/root/usr/bin" | sudo tee -a /etc/profile.d/llvm-toolset-11.sh
}

# Install system dependencies for macOS
install_macos_deps() {
    log_info "Installing Xcode command line tools..."
    xcode-select --install 2>/dev/null || true

    log_info "Installing Homebrew dependencies..."
    if ! command -v brew &> /dev/null; then
        log_error "Homebrew is required but not installed. Please install Homebrew first: https://brew.sh"
        exit 1
    fi

    brew install boost libomp ninja cmake llvm@15 ccache grep pkg-config zip unzip tbb
    brew update && brew upgrade && brew cleanup

    if [[ $(arch) == 'arm64' ]]; then
        brew install openssl librdkafka
    fi

    sudo ln -sf "$(brew --prefix llvm@15)" "/usr/local/opt/llvm" 2>/dev/null || true
    export PATH="/usr/local/opt/grep/libexec/gnubin:$PATH"
}

# Check and install Go
install_go() {
    log_info "Checking Go installation..."
    
    if command -v go &> /dev/null; then
        GO_VERSION=$(go version | awk '{print $3}' | sed 's/go//')
        REQUIRED_VERSION="1.21"
        
        if [[ "$(printf '%s\n' "$REQUIRED_VERSION" "$GO_VERSION" | sort -V | head -n1)" == "$REQUIRED_VERSION" ]]; then
            log_success "Go $GO_VERSION is already installed and meets requirements (>= $REQUIRED_VERSION)"
            return 0
        else
            log_warning "Go $GO_VERSION is installed but doesn't meet requirements (>= $REQUIRED_VERSION)"
        fi
    fi

    log_info "Installing Go..."
    GO_VERSION_TO_INSTALL="1.24.4"
    GO_ARCH="amd64"
    if [[ "$OS" == "macos" ]] && [[ $(arch) == 'arm64' ]]; then
        GO_ARCH="arm64"
    fi

    GO_TARBALL="go${GO_VERSION_TO_INSTALL}.linux-${GO_ARCH}.tar.gz"
    if [[ "$OS" == "macos" ]]; then
        GO_TARBALL="go${GO_VERSION_TO_INSTALL}.darwin-${GO_ARCH}.tar.gz"
    fi

    cd /tmp
    wget -q "https://golang.org/dl/${GO_TARBALL}"
    sudo rm -rf /usr/local/go
    sudo tar -C /usr/local -xzf "${GO_TARBALL}"
    rm "${GO_TARBALL}"

    # Add Go to PATH if not already there
    if ! echo "$PATH" | grep -q "/usr/local/go/bin"; then
        echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
        echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.profile
        export PATH=$PATH:/usr/local/go/bin
    fi

    log_success "Go $GO_VERSION_TO_INSTALL installed successfully"
}

# Check and install CMake
install_cmake() {
    log_info "Checking CMake installation..."
    
    if command -v cmake &> /dev/null; then
        CMAKE_VERSION=$(cmake --version | head -1 | awk '{print $3}')
        REQUIRED_VERSION="3.26"
        
        if [[ "$(printf '%s\n' "$REQUIRED_VERSION" "$CMAKE_VERSION" | sort -V | head -n1)" == "$REQUIRED_VERSION" ]]; then
            log_success "CMake $CMAKE_VERSION is already installed and meets requirements (>= $REQUIRED_VERSION)"
            return 0
        else
            log_warning "CMake $CMAKE_VERSION is installed but doesn't meet requirements (>= $REQUIRED_VERSION)"
        fi
    fi

    if [[ "$OS" == "macos" ]]; then
        log_info "Installing CMake via Homebrew..."
        brew install cmake
    else
        log_info "Installing CMake..."
        CMAKE_VERSION_TO_INSTALL="3.26.5"
        ARCH=$(uname -m)
        cd /tmp
        wget -q "https://cmake.org/files/v3.26/cmake-${CMAKE_VERSION_TO_INSTALL}-linux-${ARCH}.tar.gz"
        sudo tar --strip-components=1 -xz -C /usr/local -f "cmake-${CMAKE_VERSION_TO_INSTALL}-linux-${ARCH}.tar.gz"
        rm "cmake-${CMAKE_VERSION_TO_INSTALL}-linux-${ARCH}.tar.gz"
    fi

    log_success "CMake installed successfully"
}

# Install Rust
install_rust() {
    log_info "Checking Rust installation..."
    
    if command -v cargo &> /dev/null; then
        RUST_VERSION=$(rustc --version | awk '{print $2}')
        log_success "Rust $RUST_VERSION is already installed"
        rustup install 1.89 2>/dev/null || true
        rustup default 1.89
        return 0
    fi

    log_info "Installing Rust..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain=1.89 -y
    source ~/.cargo/env || source /usr/local/cargo/env || true
    
    log_success "Rust installed successfully"
}

# Install Python dependencies
install_python_deps() {
    log_info "Setting up Python virtual environment and dependencies..."
    
    # Create virtual environment if it doesn't exist
    VENV_PATH="$HOME/.milvus-build-env"
    if [[ ! -d "$VENV_PATH" ]]; then
        python3 -m venv "$VENV_PATH"
    fi
    
    # Activate virtual environment and install conan
    source "$VENV_PATH/bin/activate"
    pip install --upgrade pip
    pip install conan==1.64.1
    
    # Add venv to PATH in shell profiles
    VENV_EXPORT="export PATH=\"$VENV_PATH/bin:\$PATH\""
    if ! grep -q "$VENV_EXPORT" ~/.bashrc 2>/dev/null; then
        echo "$VENV_EXPORT" >> ~/.bashrc
    fi
    if ! grep -q "$VENV_EXPORT" ~/.profile 2>/dev/null; then
        echo "$VENV_EXPORT" >> ~/.profile
    fi
    
    export PATH="$VENV_PATH/bin:$PATH"
    log_success "Python dependencies installed in virtual environment: $VENV_PATH"
}

# Set up environment
setup_environment() {
    log_info "Setting up build environment..."
    
    # Source cargo environment
    if [[ -f ~/.cargo/env ]]; then
        source ~/.cargo/env
    elif [[ -f /usr/local/cargo/env ]]; then
        source /usr/local/cargo/env
    fi
    
    # Set up environment variables
    ENV_SETUP="
# Milvus build environment
export PATH=\"$HOME/.milvus-build-env/bin:\$PATH\"
export PATH=\"\$PATH:/usr/local/go/bin\"
if [[ -f ~/.cargo/env ]]; then
    source ~/.cargo/env
elif [[ -f /usr/local/cargo/env ]]; then
    source /usr/local/cargo/env
fi
"

    # Add to .bashrc if not already there
    if ! grep -q "Milvus build environment" ~/.bashrc 2>/dev/null; then
        echo "$ENV_SETUP" >> ~/.bashrc
    fi
    
    # Add to .profile if not already there
    if ! grep -q "Milvus build environment" ~/.profile 2>/dev/null; then
        echo "$ENV_SETUP" >> ~/.profile
    fi
    
    log_success "Environment setup completed"
}

# Clean up any existing conan profiles that might cause issues
cleanup_conan() {
    log_info "Cleaning up existing Conan profiles..."
    rm -rf ~/.conan 2>/dev/null || true
    log_success "Conan cleanup completed"
}

# Verify installation
verify_installation() {
    log_info "Verifying installation..."
    
    # Source the environment
    source ~/.bashrc 2>/dev/null || true
    if [[ -f ~/.cargo/env ]]; then
        source ~/.cargo/env
    elif [[ -f /usr/local/cargo/env ]]; then
        source /usr/local/cargo/env
    fi
    export PATH="$HOME/.milvus-build-env/bin:$PATH"
    
    ERRORS=0
    
    # Check Go
    if command -v go &> /dev/null; then
        GO_VERSION=$(go version | awk '{print $3}')
        log_success "Go: $GO_VERSION"
    else
        log_error "Go not found in PATH"
        ERRORS=$((ERRORS + 1))
    fi
    
    # Check CMake
    if command -v cmake &> /dev/null; then
        CMAKE_VERSION=$(cmake --version | head -1 | awk '{print $3}')
        log_success "CMake: $CMAKE_VERSION"
    else
        log_error "CMake not found in PATH"
        ERRORS=$((ERRORS + 1))
    fi
    
    # Check GCC
    if command -v gcc &> /dev/null; then
        GCC_VERSION=$(gcc --version | head -1)
        log_success "GCC: $GCC_VERSION"
    else
        log_error "GCC not found in PATH"
        ERRORS=$((ERRORS + 1))
    fi
    
    # Check Rust
    if command -v cargo &> /dev/null; then
        RUST_VERSION=$(rustc --version)
        log_success "Rust: $RUST_VERSION"
    else
        log_error "Rust/Cargo not found in PATH"
        ERRORS=$((ERRORS + 1))
    fi
    
    # Check Conan
    if command -v conan &> /dev/null; then
        CONAN_VERSION=$(conan --version)
        log_success "Conan: $CONAN_VERSION"
    else
        log_error "Conan not found in PATH"
        ERRORS=$((ERRORS + 1))
    fi
    
    if [[ $ERRORS -eq 0 ]]; then
        log_success "All dependencies verified successfully!"
        return 0
    else
        log_error "$ERRORS verification errors found"
        return 1
    fi
}

# Main installation function
main() {
    log_info "Starting Milvus build environment setup..."
    
    check_root
    check_sudo
    detect_os
    
    # Install system dependencies based on OS
    case $OS in
        ubuntu)
            install_ubuntu_deps
            ;;
        centos)
            install_centos_deps
            ;;
        macos)
            install_macos_deps
            ;;
    esac
    
    # Install language runtimes and tools
    install_go
    install_cmake
    install_rust
    install_python_deps
    
    # Setup environment
    setup_environment
    cleanup_conan
    
    # Verify installation
    if verify_installation; then
        log_success "Milvus build environment setup completed successfully!"
        echo ""
        log_info "To use the environment, run: source ~/.bashrc"
        log_info "Then you can build Milvus with: make"
        log_info "Or run unit tests with: make build-cpp-with-unittest"
        echo ""
        log_warning "Note: You may need to restart your shell or run 'source ~/.bashrc' for all changes to take effect."
    else
        log_error "Setup completed with errors. Please check the output above."
        exit 1
    fi
}

# Run main function
main "$@"