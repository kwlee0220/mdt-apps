package mdt.misc;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import utils.Indexed;
import utils.stream.FStream;

import mdt.aas.DataType;
import mdt.aas.DataTypes;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
@Getter @Setter
@NoArgsConstructor
@JsonInclude(Include.NON_NULL)
public class JdbcTableInfo {
	private String tableName;
	private List<ColumnInfo> columnInfoList = Lists.newArrayList();

	@Getter
	@JsonInclude(Include.NON_NULL)
	public static class ColumnInfo {
		private final String name;
		@SuppressWarnings("rawtypes")
		private final DataType mdtType;
		
		@JsonCreator
		public ColumnInfo(@JsonProperty("name") String name, @JsonProperty("type") String mdtTypeStr) {
			this.name = name;
			this.mdtType = DataTypes.fromDataTypeName(mdtTypeStr);
		}
		
		@SuppressWarnings("rawtypes")
		public ColumnInfo(String name, DataType mdtType) {
			this.name = name;
			this.mdtType = mdtType;
		}
	}
	
	@JsonCreator
	public JdbcTableInfo(@JsonProperty("tableName") String tableName,
						@JsonProperty("columnInfoList") List<ColumnInfo> columnInfoList) {
		this.tableName = tableName;
		this.columnInfoList = columnInfoList;
	}
	
	public Indexed<ColumnInfo> getColumnInfo(String colName) {
		return FStream.from(this.columnInfoList)
						.zipWithIndex()
						.findFirst(idxed -> idxed.value().getName().equals(colName))
						.getOrThrow(() -> new IllegalArgumentException("column not found: " + colName));
	}
}
