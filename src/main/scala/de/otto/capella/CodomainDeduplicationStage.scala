package de.otto.capella

import de.otto.capella.MessageId
import de.otto.capella.kafka.Passthrough
import ox.flow.Flow
import pureconfig.ConfigReader

import scala.concurrent.duration.*

object CodomainDeduplicationStage:

    private def defaultConfig: CodomainDeduplicationConfig = CodomainDeduplicationConfig(1000, 20.seconds)

    extension (in: Flow[(Set[MessageId], Passthrough)])
        def deduplicateCodomainMessages(
            configOpt: Option[CodomainDeduplicationConfig]
        ): Flow[(Seq[MessageId], Seq[Passthrough])] =
            val config = configOpt.getOrElse(defaultConfig)
            if config.batchSize == 1 then
                in.map: (payload, passthrough) =>
                    (payload.toSeq, Seq(passthrough))
            else
                in
                    .groupedWithin(config.batchSize, config.batchingDuration)
                    .map: batches =>
                        val (payloads, passthroughs) = batches.unzip
                        (payloads.flatten.distinct, passthroughs.sorted)

case class CodomainDeduplicationConfig(batchSize: Int, batchingDuration: FiniteDuration) derives ConfigReader
