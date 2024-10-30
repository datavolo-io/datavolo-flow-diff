# SPDX-FileCopyrightText: 2024 Datavolo Inc.
#
# SPDX-License-Identifier: Apache-2.0
FROM maven:3.9.9-eclipse-temurin-21 as builder

COPY flow-diff /flow-diff
RUN mvn -f /flow-diff/pom.xml clean package

FROM eclipse-temurin:21-jre-noble

COPY entrypoint.sh /entrypoint.sh
COPY --from=builder /flow-diff/target/flow-diff-0.0.1.jar /flow-diff-0.0.1.jar

ENTRYPOINT ["/entrypoint.sh"]
