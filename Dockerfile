# Use Google's Debian 13 distroless image (contains glibc 2.39+)
FROM gcr.io/distroless/cc-debian13:latest

WORKDIR /data

# Use your existing pre-compiled Rust binary injection
ARG TARGETPLATFORM
COPY ./binaries/${TARGETPLATFORM}/umadb /umadb

EXPOSE 50051

ENTRYPOINT ["/umadb"]
CMD ["--listen", "0.0.0.0:50051", "--db-path", "/data"]