FROM ubuntu:24.04
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    g++ cmake libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc \
    libgtest-dev python3 ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j2 && ctest --test-dir build --output-on-failure
RUN useradd --create-home miniray
USER miniray
CMD ["./build/miniray-coordinator"]
