package de.otto.capella

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.scalalogging.LazyLogging
import de.otto.capella.ChannelName
import de.otto.capella.JsonSupport.mapper
import de.otto.capella.MessageFormatName
import de.otto.capella.MessageId
import de.otto.capella.QualifiedMessageId
import de.otto.capella.SimpleProcessingTimeLogger.measureMap
import de.otto.capella.kafka.Passthrough
import de.otto.capella.statestore.StateStore
import de.otto.capella.statestore.StateStoreSection
import de.otto.capella.util.ExceptionUtil.stackTraceAsString
import org.rocksdb.RocksDBException
import ox.flow.Flow

import scala.util.control.NonFatal
import de.otto.capella.config.RelationConfigs

object CodomainCompositionStage extends LazyLogging:

    extension (in: Flow[(Seq[MessageId], Seq[Passthrough])])
        def composeCodomainMessages(
            config: RelationConfigs,
            stateStore: StateStore
        ): Flow[(Seq[MessageId], Seq[Passthrough])] =
            in.map:
                measureMap("CodomainComposition"): (payloads, passthroughs) =>
                    // println(s"+++++++++++ CodomainComposition ${payloads.map(_._1).mkString("; ")}")
                    val payloadsOut: Seq[MessageId] =
                        payloads.flatMap: codomainMessageId =>
                            try
                                println(s"+++++++++++*** CodomainComposition $codomainMessageId")
                                val codomainMessage: ObjectNode =
                                    mapper.createObjectNode().asInstanceOf[ObjectNode]

                                compose(
                                    QualifiedMessageId(config.root, codomainMessageId),
                                    codomainMessage,
                                    stateStore
                                )
                                // compose(qmid, codomainMessage, stateStore)

                                val codomainKey = s"${StateStoreSection.STA}/${codomainMessageId}"
                                if codomainMessage.isEmpty then stateStore.delete(codomainKey)
                                else stateStore.putJson(codomainKey, codomainMessage)

                                Some(codomainMessageId)
                            catch
                                case e: RocksDBException =>
                                    throw e
                                case NonFatal(ex) =>
                                    logger.error(
                                        s"Error processing codomain message ($codomainMessageId): ${ex.stackTraceAsString}"
                                    )
                                    None
                    (payloadsOut, passthroughs)

    private def compose(
        currentDomainMessageId: QualifiedMessageId,
        codomainMessage: ObjectNode,
        stateStore: StateStore
    ): Unit =
        println(s"compose current $currentDomainMessageId")
        val currentDomainMessageOpt: Option[JsonNode] =
            stateStore.getJson(s"${StateStoreSection.DOM}/$currentDomainMessageId")
        val currentDomainMessageMap: ObjectNode =
            Option(codomainMessage.get(currentDomainMessageId.qualifierString))
                .fold(mapper.createObjectNode())(_.asInstanceOf[ObjectNode])
        setObject(currentDomainMessageMap, currentDomainMessageId.id.toString, currentDomainMessageOpt)
        setObject(
            codomainMessage,
            currentDomainMessageId.qualifierString,
            if currentDomainMessageMap.isEmpty then None else Some(currentDomainMessageMap)
        )

        val next: Set[QualifiedMessageId] =
            stateStore.getStringSet(s"${StateStoreSection.LNK}/$currentDomainMessageId").map(QualifiedMessageId(_))

        next.foreach: nextMessageId =>
            println(s"LINK FROM $currentDomainMessageId TO $nextMessageId")
            compose(nextMessageId, codomainMessage, stateStore)

    private def setObject(parent: ObjectNode, name: String, objOpt: Option[JsonNode]): Unit =
        objOpt match
            case Some(obj) =>
                parent.set[JsonNode](name, obj)
            case None =>
                parent.remove(name)
