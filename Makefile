# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.PHONY: build-34-212
build-34-212:
	./mvnw spotless:apply install -pl lance-spark-3.4_2.12 -am

.PHONY: build-34-213
build-34-213:
	./mvnw spotless:apply install -pl lance-spark-3.4_2.13 -am

.PHONY: build-35-212
build-35-212:
	./mvnw spotless:apply install -pl lance-spark-3.5_2.12 -am

.PHONY: build-35-213
build-35-213:
	./mvnw spotless:apply install -pl lance-spark-3.5_2.13 -am

.PHONY: build-40-212
build-40-212:
	./mvnw spotless:apply install -pl lance-spark-4.0_2.12 -am

.PHONY: build-40-213
build-40-213:
	./mvnw spotless:apply install -pl lance-spark-4.0_2.13 -am

# Clean targets for each Spark/Scala combination
.PHONY: clean-34-212
clean-34-212:
	./mvnw clean -pl lance-spark-3.4_2.12

.PHONY: clean-34-213
clean-34-213:
	./mvnw clean -pl lance-spark-3.4_2.13

.PHONY: clean-35-212
clean-35-212:
	./mvnw clean -pl lance-spark-3.5_2.12

.PHONY: clean-35-213
clean-35-213:
	./mvnw clean -pl lance-spark-3.5_2.13

.PHONY: clean-40-212
clean-40-212:
	./mvnw clean -pl lance-spark-4.0_2.12

.PHONY: clean-40-213
clean-40-213:
	./mvnw clean -pl lance-spark-4.0_2.13

.PHONY: bundle-34-212
bundle-34-212:
	./mvnw install -pl lance-spark-bundle-3.4_2.12 -am

.PHONY: bundle-34-213
bundle-34-213:
	./mvnw install -pl lance-spark-bundle-3.4_2.13 -am

.PHONY: bundle-35-212
bundle-35-212:
	./mvnw install -pl lance-spark-bundle-3.5_2.12 -am

.PHONY: bundle-35-213
bundle-35-213:
	./mvnw install -pl lance-spark-bundle-3.5_2.13 -am

.PHONY: bundle-40-212
bundle-40-212:
	./mvnw install -pl lance-spark-bundle-4.0_2.12 -am

.PHONY: bundle-40-213
bundle-40-213:
	./mvnw install -pl lance-spark-bundle-4.0_2.13 -am

.PHONY: build
build:
	./mvnw spotless:apply install

.PHONY: clean
clean:
	./mvnw clean

.PHONY: build-base-212
build-base:
	./mvnw spotless:apply install -pl lance-spark-base_2.12 -am

.PHONY: build-base-213
build-base:
	./mvnw spotless:apply install -pl lance-spark-base_2.13 -am

.PHONY: docker-build
docker-build: bundle-35-212
	cp lance-spark-bundle-3.5_2.12/target/lance-spark-bundle-3.5_2.12-*.jar docker/
	cd docker && docker compose build --no-cache spark-lance

.PHONY: docker-up
docker-up:
	cd docker && docker-compose up -d

.PHONY: docker-shell
docker-shell:
	cd docker && docker exec -it spark-lance bash

.PHONY: docker-down
docker-down:
	cd docker && docker-compose down

.PHONY: serve-docs
serve-docs:
	cd docs && uv pip install -r requirements.txt && uv run mkdocs serve