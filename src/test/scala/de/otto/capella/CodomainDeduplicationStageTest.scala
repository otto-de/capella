package de.otto.capella

import de.otto.capella.ChannelName
import de.otto.capella.CodomainDeduplicationConfig
import de.otto.capella.CodomainDeduplicationStage.deduplicateCodomainMessages
import de.otto.capella.MessageId
import de.otto.capella.TestUtils.mockedEmptyKafkaRecord
import de.otto.capella.kafka.Passthrough
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.flow.Flow

import scala.concurrent.duration.*

class CodomainDeduplicationStageTest extends AnyFlatSpec, Matchers, Diagrams:

    private val domainX = ChannelName("domain-x")

    private val aggregateIdXA1 = MessageId("d9708e05-e6a6-4d38-b452-44dd00856374")
    private val aggregateIdXA2 = MessageId("cc5b5bd9-cded-4357-91b2-f8fdd4a3e0c9")
    private val aggregateIdXA3 = MessageId("789023e5-2974-4dc3-877d-9f975305235b")
    private val passXA11 = mockedEmptyKafkaRecord(aggregateIdXA1.toString, domainX.toString, 1, 1)
    private val passXA12 = mockedEmptyKafkaRecord(aggregateIdXA1.toString, domainX.toString, 1, 7)
    private val passXA21 = mockedEmptyKafkaRecord(aggregateIdXA2.toString, domainX.toString, 1, 2)
    private val passXA22 = mockedEmptyKafkaRecord(aggregateIdXA2.toString, domainX.toString, 1, 8)
    private val passXA31 = mockedEmptyKafkaRecord(aggregateIdXA3.toString, domainX.toString, 1, 3)
    private val passXA32 = mockedEmptyKafkaRecord(aggregateIdXA3.toString, domainX.toString, 1, 9)
    // for the sake of simplicity lets assume domain name == topic name here

    private val aggregateIdXB1 = MessageId("a00e158e-6c15-45f9-8db7-734435dcaf20")
    private val aggregateIdXB2 = MessageId("e1bbd848-da71-4779-a930-e785ff3eb02b")
    private val aggregateIdXB3 = MessageId("a9e872aa-3c13-4942-8f67-7ab5c18e7d01")
    private val passXB11 = mockedEmptyKafkaRecord(aggregateIdXB1.toString, domainX.toString, 1, 4)
    private val passXB12 = mockedEmptyKafkaRecord(aggregateIdXB1.toString, domainX.toString, 1, 10)
    private val passXB21 = mockedEmptyKafkaRecord(aggregateIdXB2.toString, domainX.toString, 1, 5)
    private val passXB22 = mockedEmptyKafkaRecord(aggregateIdXB2.toString, domainX.toString, 1, 11)
    private val passXB31 = mockedEmptyKafkaRecord(aggregateIdXB3.toString, domainX.toString, 1, 6)
    private val passXB32 = mockedEmptyKafkaRecord(aggregateIdXB3.toString, domainX.toString, 1, 12)

    private val domainY = ChannelName("domain-y")

    private val aggregateIdYA1 = MessageId("ce02add2-0932-448d-b753-acb3ed116663")
    private val aggregateIdYA2 = MessageId("c433a2d5-ed0c-412b-957b-72f61db9f24d")
    private val aggregateIdYA3 = MessageId("affe28af-8cf3-4e67-be73-0463fcae84b7")
    private val passYA1 = mockedEmptyKafkaRecord(aggregateIdYA1.toString, domainY.toString, 1, 1)
    private val passYA2 = mockedEmptyKafkaRecord(aggregateIdYA2.toString, domainY.toString, 1, 2)
    private val passYA3 = mockedEmptyKafkaRecord(aggregateIdYA3.toString, domainY.toString, 1, 3)

    private val coId01 = MessageId("c556f4bf-297c-4ded-8310-2dc8ffcab47a")
    private val coId02 = MessageId("77dc2f1c-6c27-4acc-bf0e-bec8e144e188")
    private val coId03 = MessageId("16c2a62a-6bd8-4076-8e43-f1c88f3402ec")

    "CodomainDeduplicationStage" should "do not deduplicate when batch size = 1" in:

        // given
        val config = CodomainDeduplicationConfig(1, 100.millis)

        val src: Flow[(Seq[MessageId], Passthrough)] =
            Flow.fromValues(
                (Seq(coId01), passXA11),
                (Seq(coId01), passXA21),
                (Seq(coId01), passXA31)
            )

        // when
        val result: Seq[(Seq[MessageId], Seq[Passthrough])] =
            src
                .deduplicateCodomainMessages(Some(config))
                .runToList()

        // then
        assert(result.size == 3)
        val result0 = result(0)
        val result1 = result(1)
        val result2 = result(2)
        assert(result0._1 == Seq(coId01))
        assert(result1._1 == Seq(coId01))
        assert(result2._1 == Seq(coId01))
        assert(result0._2 == Seq(passXA11))
        assert(result1._2 == Seq(passXA21))
        assert(result2._2 == Seq(passXA31))

    it should "batch but not deduplicate when aggregate ids are distinct" in:

        // given
        val config = CodomainDeduplicationConfig(3, 100.millis)

        val src: Flow[(Seq[MessageId], Passthrough)] =
            Flow.fromValues(
                (Seq(coId01), passXA11),
                (Seq(coId02), passXA21),
                (Seq(coId03), passXA31)
            )

        // when
        val result: Seq[(Seq[MessageId], Seq[Passthrough])] =
            src
                .deduplicateCodomainMessages(Some(config))
                .runToList()

        // then
        assert(result.size == 1)
        result(0)._1 should contain theSameElementsAs Seq(coId01, coId02, coId03) // no ordering guarantees!
        result(0)._2 should contain theSameElementsAs Seq(passXA11, passXA21, passXA31)

    it should "batch and deduplicate in one window" in:

        // given
        val config = CodomainDeduplicationConfig(15, 100.millis)

        val src: Flow[(Seq[MessageId], Passthrough)] =
            Flow.fromValues(
                (Seq(coId01), passXA11),
                (Seq(coId02), passXA21),
                (Seq(coId03), passXA31),
                (Seq(coId01), passXB11),
                (Seq(coId02), passXB21),
                (Seq(coId03), passXB31),
                (Seq(coId01), passYA1),
                (Seq(coId02), passYA2),
                (Seq(coId03), passYA3),
                (Seq(coId01), passXA12),
                (Seq(coId02), passXA22),
                (Seq(coId03), passXA32),
                (Seq(coId01), passXB12),
                (Seq(coId02), passXB22),
                (Seq(coId03), passXB32)
            )

        // when
        val result: Seq[(Seq[MessageId], Seq[Passthrough])] =
            src
                .deduplicateCodomainMessages(Some(config))
                .runToList()

        // then
        assert(result.size == 1)
        result(0)._1 should contain theSameElementsAs // no ordering guarantees!
            Seq(
                coId01,
                coId02,
                coId03
            )
        result(0)._2 should contain theSameElementsInOrderAs // ordered by topic, then partition and then offset
            Seq(
                passXA11,
                passXA21,
                passXA31,
                passXB11,
                passXB21,
                passXB31,
                passXA12,
                passXA22,
                passXA32,
                passXB12,
                passXB22,
                passXB32,
                passYA1,
                passYA2,
                passYA3
            )

    it should "batch and deduplicate in multiple windows" in:

        // given
        val config = CodomainDeduplicationConfig(4, 100.millis)

        val src: Flow[(Seq[MessageId], Passthrough)] =
            Flow.fromValues(
                (Seq(coId01), passXA11),
                (Seq(coId02), passXA21),
                (Seq(coId03), passXA31),
                (Seq(coId01), passXB11),
                (Seq(coId02), passXB21),
                (Seq(coId03), passXB31),
                (Seq(coId01), passYA1),
                (Seq(coId02), passYA2),
                (Seq(coId03), passYA3),
                (Seq(coId01), passXA12),
                (Seq(coId02), passXA22),
                (Seq(coId03), passXA32),
                (Seq(coId01), passXB12),
                (Seq(coId02), passXB22),
                (Seq(coId03), passXB32)
            )

        // when
        val result: Seq[(Seq[MessageId], Seq[Passthrough])] =
            src
                .deduplicateCodomainMessages(Some(config))
                .runToList()

        // then
        assert(result.size == 4)
        result(0)._1 should contain theSameElementsAs
            Seq(
                coId01,
                coId02,
                coId03
            )

        result(0)._2 should contain theSameElementsInOrderAs
            Seq(
                passXA11,
                passXA21,
                passXA31,
                passXB11
            )

        result(1)._1 should contain theSameElementsAs
            Seq(
                coId01,
                coId02,
                coId03
            )

        result(1)._2 should contain theSameElementsInOrderAs
            Seq(
                passXB21,
                passXB31,
                passYA1,
                passYA2
            )

        result(2)._1 should contain theSameElementsAs
            Seq(
                coId01,
                coId02,
                coId03
            )

        result(2)._2 should contain theSameElementsInOrderAs
            Seq(
                passXA12,
                passXA22,
                passXA32,
                passYA3
            )

        result(3)._1 should contain theSameElementsAs
            Seq(
                coId01,
                coId02,
                coId03
            )

        result(3)._2 should contain theSameElementsInOrderAs
            Seq(
                passXB12,
                passXB22,
                passXB32
            )
