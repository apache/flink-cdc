-- Copyright 2022 Ververica Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--   http://www.apache.org/licenses/LICENSE-2.0
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  charset_test
-- ----------------------------------------------------------------------------------------------------------------

CREATE TABLE `ascii_test` (
  `table_id` bigint NOT NULL AUTO_INCREMENT COMMENT '编号',
  `table_name` varchar(200) CHARACTER SET ascii DEFAULT '' COMMENT '表名称',
  PRIMARY KEY (`table_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=ascii;

CREATE TABLE `big5_test` (
  `table_id` bigint NOT NULL AUTO_INCREMENT COMMENT '编号',
  `table_name` varchar(200) CHARACTER SET big5 DEFAULT '' COMMENT '表名称',
  PRIMARY KEY (`table_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=big5;

CREATE TABLE `gbk_test` (
  `table_id` bigint NOT NULL AUTO_INCREMENT COMMENT '编号',
  `table_name` varchar(200) CHARACTER SET gbk DEFAULT '' COMMENT '表名称',
  PRIMARY KEY (`table_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=gbk;

CREATE TABLE `sjis_test` (
  `table_id` bigint NOT NULL AUTO_INCREMENT COMMENT '编号',
  `table_name` varchar(200) CHARACTER SET sjis DEFAULT '' COMMENT '表名称',
  PRIMARY KEY (`table_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=sjis;

CREATE TABLE `cp932_test` (
  `table_id` bigint NOT NULL AUTO_INCREMENT COMMENT '编号',
  `table_name` varchar(200) CHARACTER SET cp932 DEFAULT '' COMMENT '表名称',
  PRIMARY KEY (`table_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=cp932;

CREATE TABLE `gb2312_test` (
  `table_id` bigint NOT NULL AUTO_INCREMENT COMMENT '编号',
  `table_name` varchar(200) CHARACTER SET gb2312 DEFAULT '' COMMENT '表名称',
  PRIMARY KEY (`table_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=gb2312;

CREATE TABLE `ujis_test` (
  `table_id` bigint NOT NULL AUTO_INCREMENT COMMENT '编号',
  `table_name` varchar(200) CHARACTER SET ujis DEFAULT '' COMMENT '表名称',
  PRIMARY KEY (`table_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=ujis;

CREATE TABLE `euckr_test` (
  `table_id` bigint NOT NULL AUTO_INCREMENT COMMENT '编号',
  `table_name` varchar(200) CHARACTER SET euckr DEFAULT '' COMMENT '表名称',
  PRIMARY KEY (`table_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=euckr;

CREATE TABLE `latin1_test` (
  `table_id` bigint NOT NULL AUTO_INCREMENT COMMENT '编号',
  `table_name` varchar(200) CHARACTER SET latin1 DEFAULT '' COMMENT '表名称',
  PRIMARY KEY (`table_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=latin1;


CREATE TABLE `latin2_test` (
  `table_id` bigint NOT NULL AUTO_INCREMENT COMMENT '编号',
  `table_name` varchar(200) CHARACTER SET latin2 DEFAULT '' COMMENT '表名称',
  PRIMARY KEY (`table_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=latin2;


CREATE TABLE `greek_test` (
  `table_id` bigint NOT NULL AUTO_INCREMENT COMMENT '编号',
  `table_name` varchar(200) CHARACTER SET greek DEFAULT '' COMMENT '表名称',
  PRIMARY KEY (`table_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=greek;


CREATE TABLE `hebrew_test` (
  `table_id` bigint NOT NULL AUTO_INCREMENT COMMENT '编号',
  `table_name` varchar(200) CHARACTER SET hebrew DEFAULT '' COMMENT '表名称',
  PRIMARY KEY (`table_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=hebrew;


CREATE TABLE `cp866_test` (
  `table_id` bigint NOT NULL AUTO_INCREMENT COMMENT '编号',
  `table_name` varchar(200) CHARACTER SET cp866 DEFAULT '' COMMENT '表名称',
  PRIMARY KEY (`table_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=cp866;


CREATE TABLE `tis620_test` (
  `table_id` bigint NOT NULL AUTO_INCREMENT COMMENT '编号',
  `table_name` varchar(200) CHARACTER SET tis620 DEFAULT '' COMMENT '表名称',
  PRIMARY KEY (`table_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=tis620;


CREATE TABLE `cp1250_test` (
  `table_id` bigint NOT NULL AUTO_INCREMENT COMMENT '编号',
  `table_name` varchar(200) CHARACTER SET cp1250 DEFAULT '' COMMENT '表名称',
  PRIMARY KEY (`table_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=cp1250;

CREATE TABLE `cp1251_test` (
  `table_id` bigint NOT NULL AUTO_INCREMENT COMMENT '编号',
  `table_name` varchar(200) CHARACTER SET cp1251 DEFAULT '' COMMENT '表名称',
  PRIMARY KEY (`table_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=cp1251;

CREATE TABLE `cp1257_test` (
  `table_id` bigint NOT NULL AUTO_INCREMENT COMMENT '编号',
  `table_name` varchar(200) CHARACTER SET cp1257 DEFAULT '' COMMENT '表名称',
  PRIMARY KEY (`table_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=cp1257;

CREATE TABLE `macroman_test` (
  `table_id` bigint NOT NULL AUTO_INCREMENT COMMENT '编号',
  `table_name` varchar(200) CHARACTER SET macroman DEFAULT '' COMMENT '表名称',
  PRIMARY KEY (`table_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=macroman;

CREATE TABLE `macce_test` (
  `table_id` bigint NOT NULL AUTO_INCREMENT COMMENT '编号',
  `table_name` varchar(200) CHARACTER SET macce DEFAULT '' COMMENT '表名称',
  PRIMARY KEY (`table_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=macce;

CREATE TABLE `utf8_test` (
  `table_id` bigint NOT NULL AUTO_INCREMENT COMMENT '编号',
  `table_name` varchar(200) CHARACTER SET utf8 DEFAULT '' COMMENT '表名称',
  PRIMARY KEY (`table_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=utf8;

CREATE TABLE `ucs2_test` (
  `table_id` bigint NOT NULL AUTO_INCREMENT COMMENT '编号',
  `table_name` varchar(200) CHARACTER SET ucs2 DEFAULT '' COMMENT '表名称',
  PRIMARY KEY (`table_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=ucs2;

INSERT into `ascii_test` values (1, 'ascii test!?'), (2, 'Craig Marshall'), (3, '{test}');
INSERT into `big5_test` values (1, '大五'), (2, 'Craig Marshall'), (3, '丹店');
INSERT into `gbk_test` values (1, '测试数据'), (2, 'Craig Marshall'), (3, '另一个测试数据');
INSERT into `sjis_test` values (1, 'ひびぴ'), (2, 'Craig Marshall'), (3, 'フブプ');
INSERT into `cp932_test` values (1, 'ひびぴ'), (2, 'Craig Marshall'), (3, 'フブプ');
INSERT into `gb2312_test` values (1, '测试数据'), (2, 'Craig Marshall'), (3, '另一个测试数据');
INSERT into `ujis_test` values (1, 'ひびぴ'), (2, 'Craig Marshall'), (3, 'フブプ');
INSERT into `euckr_test` values (1, '죠주쥬'), (2, 'Craig Marshall'), (3, '한국어');
INSERT into `latin1_test` values (1, 'ÀÆÉ'), (2, 'Craig Marshall'), (3, 'Üæû');
INSERT into `latin2_test` values (1, 'ÓÔŐÖ'), (2, 'Craig Marshall'), (3, 'ŠŞŤŹ');
INSERT into `greek_test` values (1, 'αβγδε'), (2, 'Craig Marshall'), (3, 'θικλ');
INSERT into `hebrew_test` values (1, 'בבקשה'), (2, 'Craig Marshall'), (3, 'שרפה');
INSERT into `cp866_test` values (1, 'твой'), (2, 'Craig Marshall'), (3, 'любой');
INSERT into `tis620_test` values (1, 'ภาษาไทย'), (2, 'Craig Marshall'), (3, 'ฆงจฉ');
INSERT into `cp1250_test` values (1, 'ÓÔŐÖ'), (2, 'Craig Marshall'), (3, 'ŠŞŤŹ');
INSERT into `cp1251_test` values (1, 'твой'), (2, 'Craig Marshall'), (3, 'любой');
INSERT into `cp1257_test` values (1, 'piedzimst brīvi'), (2, 'Craig Marshall'), (3, 'apveltīti ar saprātu');
INSERT into `macroman_test` values (1, 'ÀÆÉ'), (2, 'Craig Marshall'), (3, 'Üæû');
INSERT into `macce_test` values (1, 'ÓÔŐÖ'), (2, 'Craig Marshall'), (3, 'ŮÚŰÜ');
INSERT into `utf8_test` values (1, '测试数据'), (2, 'Craig Marshall'), (3, '另一个测试数据');
INSERT into `ucs2_test` values (1, '测试数据'), (2, 'Craig Marshall'), (3, '另一个测试数据');
