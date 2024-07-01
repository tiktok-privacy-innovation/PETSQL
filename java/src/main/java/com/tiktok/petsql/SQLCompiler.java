/*
 * Copyright 2024 TikTok Pte. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tiktok.petsql;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Frameworks;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import java.util.List;
import java.util.Properties;

public class SQLCompiler {
  /**
   * Convert SQL statements into logical plans.
   *
   * @param sql the SQL statement to be converted.
   * @param schemaStr the schema string(Json).
   * @return the converted logical plan.
   * @throws SqlParseException if the SQL statement fails to parse.
   * @throws JsonProcessingException if JSON processing fails.
   */
  public String sqlToLogicPlan(String sql, String schemaStr)
      throws SqlParseException, JsonProcessingException {
    SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT.withCaseSensitive(false));

    SqlNode node = parser.parseStmt();

    Config tableConfig = loadJson(schemaStr);

    SchemaPlus schemaPlus = Frameworks.createRootSchema(true);

    for (final Schema schema : tableConfig.getSchemas()) {
      schemaPlus.add(schema.getName(), new AbstractTable() {
        @Override
        public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
          RelDataTypeFactory.Builder builder = relDataTypeFactory.builder();
          for (Column column : schema.getColumns()) {
            try {
              builder.add(column.getName(), getSqlTypeName(column.getType()));
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
          return builder.build();
        }
      });
    }
    CalciteConnectionConfig readerConfig =
        CalciteConnectionConfig.DEFAULT.set(CalciteConnectionProperty.CASE_SENSITIVE, "false");

    SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    CalciteCatalogReader reader = new CalciteCatalogReader(CalciteSchema.from(schemaPlus),
        CalciteSchema.from(schemaPlus).path(null), factory, readerConfig);

    SqlToRelConverter.Config config =
        SqlToRelConverter.config().withTrimUnusedFields(true).withExpand(false);

    VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(config));
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(factory));

    SqlToRelConverter converter = new SqlToRelConverter(null,
        SqlValidatorUtil.newValidator(
            SqlStdOperatorTable.instance(), reader, factory, SqlValidator.Config.DEFAULT),
        reader, cluster, StandardConvertletTable.INSTANCE, config);

    RelRoot relRoot = converter.convertQuery(node, true, true);
    RelNode relNode = relRoot.rel;
    return RelOptUtil.dumpPlan(
        "json", relNode, SqlExplainFormat.JSON, SqlExplainLevel.ALL_ATTRIBUTES);
  }

  public SqlTypeName getSqlTypeName(int value) throws Exception {
    if (value == 0) {
      return SqlTypeName.CHAR;
    } else if (value == 1) {
      return SqlTypeName.BOOLEAN;
    } else if (value == 2) {
      return SqlTypeName.INTEGER;
    } else if (value == 3) {
      return SqlTypeName.DOUBLE;
    } else {
      throw new Exception("Unkown sql type name");
    }
  }

  /**
   * Loads a JSON string and converts it into a Config object.
   *
   * @param input the JSON string to be parsed.
   * @return the parsed Config object.
   * @throws JsonProcessingException if the JSON string is not valid.
   */
  public Config loadJson(String input) throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();

    return mapper.readValue(input, Config.class);
  }
}

class Column {
  private String name;
  private int type;
  private int party;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  public int getParty() {
    return party;
  }

  public void setParty(int party) {
    this.party = party;
  }
}

class Schema {
  private List<Column> columns;
  private String name;
  private int party;

  public List<Column> getColumns() {
    return columns;
  }

  public void setColumns(List<Column> columns) {
    this.columns = columns;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getParty() {
    return party;
  }

  public void setParty(int party) {
    this.party = party;
  }
}

class Config {
  private List<Schema> schemas;

  public List<Schema> getSchemas() {
    return schemas;
  }

  public void setSchemas(List<Schema> schemas) {
    this.schemas = schemas;
  }
}
