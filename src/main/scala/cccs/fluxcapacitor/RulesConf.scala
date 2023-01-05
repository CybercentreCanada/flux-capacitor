package cccs.fluxcapacitor

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import scala.collection.Map

case class RulesConf(
    rules: Seq[RuleConf]
)

object RulesConf {
  type TagMap = Map[String, Boolean]
  type SigmaMap = Map[String, TagMap]

  def load(spec: String) = {
    val objectMapper = new ObjectMapper(new YAMLFactory())
    objectMapper.findAndRegisterModules()
    objectMapper.readValue(spec, classOf[RulesConf])
  }
}

case class RuleConf(
    rulename: String,
    description: String,
    action: String,
    tags: Seq[TagConf],
    groupby: Seq[String],
    ordered: Boolean,
    parent: String,
    child: String
)

case class TagConf(
    name: String
)
