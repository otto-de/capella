package de.otto.capella

import com.fasterxml.jackson.databind.node.ObjectNode
import de.otto.capella.JsonSupport.mapper
import io.github.embeddedkafka.EmbeddedKafka
import org.apache.commons.io.FileUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.*

import java.io.File
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.nio.file.Paths
import java.util.UUID
import scala.concurrent.duration.*

/** End-to-end test that protects against regressions and serves as a basis for reproducing errors.
  */
class AppTest extends AnyFlatSpec, Matchers, Diagrams, EmbeddedKafka, BeforeAndAfterAll:

    given StringSerializer = new StringSerializer()
    given StringDeserializer = new StringDeserializer()

    override def beforeAll(): Unit = EmbeddedKafka.start()

    override def afterAll(): Unit = EmbeddedKafka.stop()

    /** This test ensures minimal application functionality.
      */
    "App" should "aggregate messages from two source topics with minimal configuration" in:

        val tempDir: Path = Paths.get(System.getProperty("java.io.tmpdir"))
        val configDir: Path = Paths.get(URLDecoder.decode(getClass.getResource("/e2e").getFile, StandardCharsets.UTF_8))

        val additionalProps = """{"test-cluster":{}}"""

        val cliArgs =
            Vector(
                "--capella-config-file",
                s"$configDir/application-minimal.yaml",
                "--capella-additional-kafka-properties",
                additionalProps,
                "--capella-state-store-path",
                s"$tempDir/capella-data/${UUID.randomUUID}"
            )

        val mediaTopic = "media"
        val authorsTopic = "authors"

        // fill source topics (books and authors)
        publishToKafka(mediaTopic, TestData.setupBookIdEasy.toString, TestData.setupBookEasy.toJson.toPrettyString)
        sleep(200.millis)
        publishToKafka(
            authorsTopic,
            TestData.setupAuthorIdTolkien.toString,
            TestData.setupAuthorTolkien.toJson.toPrettyString
        )
        publishToKafka(
            authorsTopic,
            TestData.setupAuthorIdChristie.toString,
            TestData.setupAuthorCristie.toJson.toPrettyString
        )
        publishToKafka(
            authorsTopic,
            TestData.setupAuthorIdKnuth.toString,
            TestData.setupAuthorKnuth.toJson.toPrettyString
        )
        publishToKafka(
            authorsTopic,
            TestData.setupAuthorIdFeynman.toString,
            TestData.setupAuthorFeynman.toJson.toPrettyString
        )
        sleep(200.millis)
        publishToKafka(mediaTopic, TestData.setupBookIdNile.toString, TestData.setupBookNile.toJson.toPrettyString)
        publishToKafka(
            mediaTopic,
            TestData.setupBookIdOrientExpress.toString,
            TestData.setupBookOrientExpress.toJson.toPrettyString
        )

        // run application
        supervised:
            timeoutOption(10.seconds):
                App.run(cliArgs)

        // consume from target topic
        val aggregatedMessages = consumeNumberKeyedMessagesFrom(number = 3, topic = "rich-books")

        val expectedMessageKey0 = TestData.setupBookIdEasy.toString
        val expectedMessageValue0 = {
            val authors = {
                val authorsArray = mapper.createArrayNode()
                authorsArray.add(TestData.setupAuthorFeynman.toJson)
            }
            val book = TestData.setupBookEasy.toJson
            book
                .asInstanceOf[ObjectNode]
                .set(s"${TestData.authorsChannel}/${TestData.authorMessageFormat}", authors)
            book.toString()
        }
        val expectedMessageKey1 = TestData.setupBookIdNile.toString
        val expectedMessageValue1 = {
            val authors = {
                val authorsArray = mapper.createArrayNode()
                authorsArray.add(TestData.setupAuthorCristie.toJson)
            }
            val book = TestData.setupBookNile.toJson
            book
                .asInstanceOf[ObjectNode]
                .set(s"${TestData.authorsChannel}/${TestData.authorMessageFormat}", authors)
            book.toString()
        }
        val expectedMessageKey2 = TestData.setupBookIdOrientExpress.toString
        val expectedMessageValue2 = {
            val authors = {
                val authorsArray = mapper.createArrayNode()
                authorsArray.add(TestData.setupAuthorCristie.toJson)
            }
            val book = TestData.setupBookOrientExpress.toJson
            book
                .asInstanceOf[ObjectNode]
                .set(s"${TestData.authorsChannel}/${TestData.authorMessageFormat}", authors)
            book.toString()
        }

        // assert output
        aggregatedMessages(0)._1 shouldEqual expectedMessageKey0
        aggregatedMessages(0)._2 shouldEqual expectedMessageValue0
        aggregatedMessages(1)._1 shouldEqual expectedMessageKey1
        aggregatedMessages(1)._2 shouldEqual expectedMessageValue1
        aggregatedMessages(2)._1 shouldEqual expectedMessageKey2
        aggregatedMessages(2)._2 shouldEqual expectedMessageValue2

    it should "shut down on database error immediately" in:

        val tempDir: Path = Paths.get(System.getProperty("java.io.tmpdir"))
        val configDir: Path = Paths.get(URLDecoder.decode(getClass.getResource("/e2e").getFile, StandardCharsets.UTF_8))

        val additionalProps = """{"test-cluster":{}}"""

        val dataDir = tempDir.resolve(s"capella-data/${UUID.randomUUID}")
        FileUtils.copyDirectory(
            new File(configDir.resolve("corrupted-db-data").toString),
            new File(dataDir.toString)
        )

        val cliArgs =
            Vector(
                "--capella-config-file",
                s"$configDir/application-minimal.yaml",
                "--capella-additional-kafka-properties",
                additionalProps,
                "--capella-state-store-path",
                dataDir.toString
            )

        // run application
        val exitCode =
            supervised:
                App.run(cliArgs)

        assert(exitCode == ExitCode.Failure(10))
