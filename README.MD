# Feature
该工程主要提供了提取SQL文本中table和field信息的样本实例，以及对SQL文进行简单规则检验。核心依赖：
- Apache Calcite 
- SqlParse

为了减少中间表等噪音干扰，目前约定仅统计TABLE_PREFIX指定的前缀表；同理，在SQL文本中表字段仍然会
由于大量的别名导致无法将字段与目标表进行有效关联，故仅采集带有TABLE_PREFIX前缀，指定字段来源的filed信息。