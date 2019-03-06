#!/bin/bash -xeu
CLOG_VERSION=v0.9.3

curl -sL https://github.com/clog-tool/clog-cli/releases/download/${CLOG_VERSION}/clog-${CLOG_VERSION}-x86_64-unknown-linux-musl.tar.gz | tar xzvf -
chmod +x clog

./clog --setversion ${RELEASE_VERSION}
cat CHANGELOG.md

git commit -am 'chore(project): update CHANGELOG'
