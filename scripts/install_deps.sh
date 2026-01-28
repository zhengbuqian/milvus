#!/usr/bin/env bash

# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

function install_linux_deps() {
  if [[ -x "$(command -v apt)" ]]; then
    # for Ubuntu (22.04 and 24.04)
    sudo apt-get update

    # Determine Ubuntu version for clang packages
    UBUNTU_VERSION=$(lsb_release -rs 2>/dev/null || grep -oP 'VERSION_ID="\K[^"]+' /etc/os-release || echo "22.04")
    echo "Detected Ubuntu version: $UBUNTU_VERSION"

    # Base packages (common for all Ubuntu versions)
    sudo apt-get install -y --no-install-recommends wget curl ca-certificates gnupg2 \
      g++ gcc gdb gdbserver ninja-build git make ccache libssl-dev zlib1g-dev zip unzip \
      lcov libtool m4 autoconf automake python3 python3-pip python3-venv \
      pkg-config uuid-dev libaio-dev libopenblas-dev libgoogle-perftools-dev

    # Install clang-format and clang-tidy based on Ubuntu version
    if [[ "$UBUNTU_VERSION" == "22.04" ]] || [[ "$UBUNTU_VERSION" == "20.04" ]]; then
      sudo apt-get install -y --no-install-recommends clang-format-12 clang-tidy-12
    elif [[ "$UBUNTU_VERSION" == "24.04" ]] || [[ "$UBUNTU_VERSION" > "24" ]]; then
      # Ubuntu 24.04 uses clang-format-14/18 instead of 12
      sudo apt-get install -y --no-install-recommends clang-format clang-tidy || \
      sudo apt-get install -y --no-install-recommends clang-format-14 clang-tidy-14 || true
    else
      # Fallback: try clang-format-12 first, then generic
      sudo apt-get install -y --no-install-recommends clang-format-12 clang-tidy-12 || \
      sudo apt-get install -y --no-install-recommends clang-format clang-tidy || true
    fi

    # install tzdata non-interactively
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends tzdata

    # upgrade gcc to 12 for Ubuntu (if available and not already newer)
    if apt-cache show gcc-12 > /dev/null 2>&1; then
      CURRENT_GCC_VERSION=$(gcc -dumpversion 2>/dev/null | cut -d. -f1 || echo "0")
      if [[ "$CURRENT_GCC_VERSION" -lt 12 ]]; then
        sudo apt-get install -y gcc-12 g++-12
        sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-12 100
        sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-12 100
        sudo update-alternatives --install /usr/bin/gcov gcov /usr/bin/gcov-12 100
        echo "gcc upgraded to version 12"
      else
        echo "gcc version $CURRENT_GCC_VERSION is already >= 12, skipping upgrade"
      fi
    fi

    # Install conan 1.64.1 (required by Milvus build system)
    # Use pip with --break-system-packages for Ubuntu 23.04+ (PEP 668)
    if pip3 install conan==1.64.1 2>/dev/null; then
      echo "conan 1.64.1 installed successfully"
    elif sudo pip3 install --break-system-packages conan==1.64.1 2>/dev/null; then
      echo "conan 1.64.1 installed successfully (with --break-system-packages)"
    else
      echo "Warning: Failed to install conan via pip, trying with virtual environment..."
      python3 -m venv ~/milvus-venv
      ~/milvus-venv/bin/pip install conan==1.64.1
      # Add venv to PATH
      if ! grep -q 'milvus-venv' ~/.bashrc 2>/dev/null; then
        echo 'export PATH="$HOME/milvus-venv/bin:$PATH"' >> ~/.bashrc
      fi
      export PATH="$HOME/milvus-venv/bin:$PATH"
      echo "conan 1.64.1 installed in virtual environment ~/milvus-venv"
    fi
  elif [[ -x "$(command -v yum)" ]]; then
    # for CentOS devtoolset-11
    sudo yum install -y epel-release centos-release-scl-rh
    sudo yum install -y wget curl which \
      git make automake python3-devel \
      devtoolset-11-gcc devtoolset-11-gcc-c++ devtoolset-11-gcc-gfortran devtoolset-11-libatomic-devel \
      llvm-toolset-11.0-clang llvm-toolset-11.0-clang-tools-extra openblas-devel \
      libaio libuuid-devel zip unzip \
      ccache lcov libtool m4 autoconf automake

    sudo pip3 install conan==1.64.1
    echo "source scl_source enable devtoolset-11" | sudo tee -a /etc/profile.d/devtoolset-11.sh
    echo "source scl_source enable llvm-toolset-11.0" | sudo tee -a /etc/profile.d/llvm-toolset-11.sh
    echo "export CLANG_TOOLS_PATH=/opt/rh/llvm-toolset-11.0/root/usr/bin" | sudo tee -a /etc/profile.d/llvm-toolset-11.sh
    source "/etc/profile.d/llvm-toolset-11.sh"
  else
    echo "Error Install Dependencies ..."
    exit 1
  fi

  # install Go
  GO_VERSION="1.24.11"
  if command -v go &> /dev/null; then
    current_go_version=$(go version | grep -oP 'go\K[0-9]+\.[0-9]+\.[0-9]+' || echo "0")
    echo "Current Go version: $current_go_version"
  else
    current_go_version="0"
  fi

  if [[ "$current_go_version" != "$GO_VERSION" ]]; then
    echo "Installing Go $GO_VERSION ..."
    ARCH=$(uname -m)
    case $ARCH in
      x86_64) GO_ARCH="amd64" ;;
      aarch64) GO_ARCH="arm64" ;;
      *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
    esac
    sudo mkdir -p /usr/local/go
    wget -qO- "https://go.dev/dl/go${GO_VERSION}.linux-${GO_ARCH}.tar.gz" | sudo tar --strip-components=1 -xz -C /usr/local/go

    # Set up Go environment variables
    if ! grep -q 'GOROOT=/usr/local/go' ~/.bashrc 2>/dev/null; then
      {
        echo ''
        echo '# Go environment'
        echo 'export GOROOT=/usr/local/go'
        echo 'export GOPATH=$HOME/go'
        echo 'export GO111MODULE=on'
        echo 'export PATH=$GOPATH/bin:$GOROOT/bin:$PATH'
      } >> ~/.bashrc
    fi
    export GOROOT=/usr/local/go
    export GOPATH=$HOME/go
    export GO111MODULE=on
    export PATH=$GOPATH/bin:$GOROOT/bin:$PATH
    mkdir -p "$GOPATH/src" "$GOPATH/bin"
    echo "Go $GO_VERSION installed successfully"
  else
    echo "Go $GO_VERSION already installed"
  fi

  # install cmake
  CMAKE_VERSION="3.31.8"
  cmake_current_version=$(cmake --version 2>/dev/null | head -1 | grep -oP '[0-9]+\.[0-9]+\.[0-9]+' || echo "0")
  if [[ "$cmake_current_version" != "$CMAKE_VERSION" ]]; then
    echo "Installing CMake $CMAKE_VERSION ..."
    wget -qO- "https://cmake.org/files/v3.31/cmake-${CMAKE_VERSION}-linux-$(uname -m).tar.gz" | sudo tar --strip-components=1 -xz -C /usr/local
    echo "CMake $CMAKE_VERSION installed successfully"
  else
    echo "CMake $CMAKE_VERSION already installed"
  fi

  # install rust
  RUST_VERSION="1.89"
  if command -v cargo >/dev/null 2>&1; then
      echo "cargo exists, updating to Rust $RUST_VERSION"
      rustup install $RUST_VERSION
      rustup default $RUST_VERSION
  else
      echo "Installing Rust $RUST_VERSION ..."
      curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain=$RUST_VERSION -y || { echo 'rustup install failed'; exit 1; }
      source "$HOME/.cargo/env"
  fi
}

function install_mac_deps() {
  sudo xcode-select --install > /dev/null 2>&1
  brew install boost libomp ninja cmake llvm@15 ccache grep pkg-config zip unzip tbb
  export PATH="/usr/local/opt/grep/libexec/gnubin:$PATH"
  brew update && brew upgrade && brew cleanup

  pip3 install conan==1.64.1

  if [[ $(arch) == 'arm64' ]]; then
    brew install openssl
    brew install librdkafka
  fi

  sudo ln -s "$(brew --prefix llvm@15)" "/usr/local/opt/llvm" 2>/dev/null || true

  # install Go
  GO_VERSION="1.24.11"
  if command -v go &> /dev/null; then
    current_go_version=$(go version | grep -oE 'go[0-9]+\.[0-9]+\.[0-9]+' | sed 's/go//' || echo "0")
    echo "Current Go version: $current_go_version"
  else
    current_go_version="0"
  fi

  if [[ "$current_go_version" != "$GO_VERSION" ]]; then
    echo "Installing Go $GO_VERSION via brew ..."
    brew install go@1.24 || brew upgrade go@1.24 || true
    brew link go@1.24 --force || true
    # If brew go version doesn't match, download manually
    if ! go version 2>/dev/null | grep -q "$GO_VERSION"; then
      ARCH=$(uname -m)
      case $ARCH in
        x86_64) GO_ARCH="amd64" ;;
        arm64) GO_ARCH="arm64" ;;
        *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
      esac
      sudo rm -rf /usr/local/go
      wget -qO- "https://go.dev/dl/go${GO_VERSION}.darwin-${GO_ARCH}.tar.gz" | sudo tar -xz -C /usr/local
    fi
    echo "Go installed successfully"
  else
    echo "Go $GO_VERSION already installed"
  fi

  # Set up Go environment variables
  if ! grep -q 'GOROOT=' ~/.zshrc 2>/dev/null && ! grep -q 'GOROOT=' ~/.bashrc 2>/dev/null; then
    SHELL_RC="$HOME/.zshrc"
    [[ -f "$HOME/.bashrc" ]] && [[ ! -f "$HOME/.zshrc" ]] && SHELL_RC="$HOME/.bashrc"
    {
      echo ''
      echo '# Go environment'
      echo 'export GOROOT=/usr/local/go'
      echo 'export GOPATH=$HOME/go'
      echo 'export GO111MODULE=on'
      echo 'export PATH=$GOPATH/bin:$GOROOT/bin:$PATH'
    } >> "$SHELL_RC"
  fi
  export GOROOT=/usr/local/go
  export GOPATH=$HOME/go
  export GO111MODULE=on
  export PATH=$GOPATH/bin:$GOROOT/bin:$PATH
  mkdir -p "$GOPATH/src" "$GOPATH/bin" 2>/dev/null || true

  # install rust
  RUST_VERSION="1.89"
  if command -v cargo >/dev/null 2>&1; then
      echo "cargo exists, updating to Rust $RUST_VERSION"
      rustup install $RUST_VERSION
      rustup default $RUST_VERSION
  else
      echo "Installing Rust $RUST_VERSION ..."
      curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain=$RUST_VERSION -y || { echo 'rustup install failed'; exit 1; }
      source "$HOME/.cargo/env"
  fi
}

unameOut="$(uname -s)"
case "${unameOut}" in
    Linux*)     install_linux_deps;;
    Darwin*)    install_mac_deps;;
    *)          echo "Unsupported OS:${unameOut}" ; exit 0;
esac

echo ""
echo "========================================"
echo "Dependencies installed successfully!"
echo "========================================"
echo ""
echo "IMPORTANT: To use the installed tools in your current shell, run:"
echo "  source ~/.bashrc  (for Linux/bash)"
echo "  source ~/.zshrc   (for macOS/zsh)"
echo ""
echo "Or start a new terminal session."
echo ""

