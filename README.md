# pySparkDemo_OpenAlex

实验室服务器已完成 OpenAlex 数据集下载及转换，过程依据[官方文档](https://docs.openalex.org/download-snapshot)。本仓库包含基于 PySpark 的 OpenAlex 读取初始化过程，可方便基于该数据集进行实验。同时可以参考仓库内的 [教程 notebook](https://github.com/whuscity/pySparkDemo_OpenAlex/blob/main/OpenAlexSparkTutorial.ipynb) 进行基本学习。

## 维护说明

### ID 转换

参照 [配置过程](https://blog.mariozzj.cn/posts/fb1f3a84/) 进行。需要特别说明：由于原始数据集中所有实体（Works、Authors、Concepts、Institutions、Venues）的 id 均为完整 url，形如 `http://openalex.org/works/W10000001`，会大大降低连接、聚合运算、匹配的计算速度，因此在维护中将其转换为纯数字 id，形如 `10000001`。如需取回原始 id，可以参考各实体对应的 ids 表 `OpenAlex` 字段。

### 序列转换

部分字段实际为序列，但转换时以字符串储存在文件中，因此对这些字段追加序列字段便于操作，追加的字段数据类型为 `pySpark.sql.types.ArrayType`：
* `Authors` 表 `display_name_alternatives` 追加 `display_name_alternatives_array` 字段。
* `ConceptsIds` 表 `umls_aui` 追加 `umls_aui_array` 字段。
* `ConceptsIds` 表 `umls_cui` 追加 `umls_cui_array` 字段。
* `Institutions` 表 `display_name_acronyms` 追加 `display_name_acronyms_array` 字段。
* `Institutions` 表 `display_name_alternatives` 追加 `display_name_alternatives_array` 字段。
* `Venues` 表 `issn` 追加 `issn_array` 字段。
* `VenuesIds` 表 `issn` 追加 `issn_array` 字段。

## 使用方法

本仓库已设置为 Github 模板仓库，可以基于 demo 创建仓库。步骤：
1. 点击页面上方 `Use This Template`，创建自己的仓库；
2. 使用 `git clone` 命令将自己的仓库克隆到到服务器上自己的文件夹内；
3. 参照 [教程 notebook](https://github.com/whuscity/pySparkDemo_OpenAlex/blob/main/OpenAlexSparkTutorial.ipynb)，在服务器上创建自己的 notebook，进行自己的实验。注意关键代码块：`%run spark_openalex_init.py --appname=UserName_OpenAlex`