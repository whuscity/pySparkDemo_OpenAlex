# pySparkDemo_OpenAlex

实验室服务器已完成 OpenAlex 数据集下载及转换，过程依据[官方文档](https://docs.openalex.org/download-snapshot)。本仓库包含基于 PySpark 的 OpenAlex 读取初始化过程，可方便基于该数据集进行实验。同时可以参考仓库内的 [教程 notebook](https://github.com/whuscity/pySparkDemo_OpenAlex/blob/main/OpenAlexSparkTutorial.ipynb) 进行基本学习。

## 使用方法

本仓库已设置为 Github 模板仓库，可以基于 demo 创建仓库。步骤：
1. 点击页面上方 `Use This Template`，创建自己的仓库；
2. 使用 `git clone` 命令将自己的仓库克隆到到服务器上自己的文件夹内；
3. 参照 demo，在服务器上创建自己的 notebook，进行自己的实验。注意关键代码块：`%run spark_openalex_init.py`