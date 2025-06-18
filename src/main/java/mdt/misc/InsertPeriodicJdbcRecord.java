package mdt.misc;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.AbstractExecutionThreadService;

import utils.jdbc.JdbcConfiguration;
import utils.jdbc.JdbcProcessor;
import utils.stream.FStream;

import de.siegmar.fastcsv.reader.CloseableIterator;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.NamedCsvRecord;
import mdt.aas.DataTypes;
import mdt.misc.JdbcTableInfo.ColumnInfo;
import me.tongfei.progressbar.ProgressBar;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class InsertPeriodicJdbcRecord extends AbstractExecutionThreadService {
	private final File m_csvFile;
	private final JdbcProcessor m_jdbc;
	private final JdbcTableInfo m_tableInfo;
	private final String m_insertSql;
	private final int m_tsColIndex;
	
	public InsertPeriodicJdbcRecord(File csvFile, JdbcConfiguration jconf, JdbcTableInfo tableInfo) {
		m_csvFile = csvFile;
		m_jdbc = JdbcProcessor.create(jconf.getJdbcUrl(), jconf.getUser(), jconf.getPassword());
		m_tableInfo = tableInfo;
		m_tsColIndex = tableInfo.getColumnInfo("ts").index();
		
		// build insert statement
		String colCsv = FStream.from(tableInfo.getColumnInfoList()).map(ColumnInfo::getName).join(',');
		String paramCsv = FStream.range(0, tableInfo.getColumnInfoList().size()).map(idx -> "?").join(',');
		m_insertSql = String.format("insert into %s(%s) values (%s)", tableInfo.getTableName(), colCsv, paramCsv);
		
	}
	
	public static final void main(String... args) throws Exception {
		File csvFile = new File("data.csv");
		
		JdbcConfiguration jconf = new JdbcConfiguration();
		jconf.setJdbcUrl("jdbc:postgresql://localhost:5432/mdt");
		jconf.setUser("mdt");
		jconf.setPassword("urc2004");
		List<ColumnInfo> colInfoList = List.of(new ColumnInfo("ts", DataTypes.DATE_TIME),
												new ColumnInfo("amphere", DataTypes.FLOAT));
		JdbcTableInfo tableInfo = new JdbcTableInfo("samcheon", colInfoList);
		
		InsertPeriodicJdbcRecord job = new InsertPeriodicJdbcRecord(csvFile, jconf, tableInfo);
		job.startAsync();
		job.awaitRunning();
	}

	@Override
	public void run() throws SQLException, ExecutionException, IOException, InterruptedException {
		long nrows = FStream.from(Files.lines(m_csvFile.toPath())).count();
		try ( ProgressBar progress = ProgressBar.builder()
												.setTaskName("Inserting...")
												.setInitialMax(nrows)
												.setUpdateIntervalMillis(1*1000)
												.build();
				CsvReader<NamedCsvRecord> reader = CsvReader.builder().ofNamedCsvRecord(m_csvFile.toPath());
				CloseableIterator<NamedCsvRecord> iter = reader.iterator() ) {
			long started = System.currentTimeMillis();
			long firstTs = -1;
			while ( iter.hasNext() ) {
				if ( !isRunning() ) {
					return;
				}
				
				NamedCsvRecord rec = iter.next();
				List<Object> columns = buildJdbcColumns(rec);
				
				Timestamp ts = (Timestamp)columns.get(m_tsColIndex);
				if ( firstTs <= 0 ) {
					firstTs = ts.getTime();
				}
				
				long elapsed = System.currentTimeMillis() - started;
				long gap = (ts.getTime() - firstTs) - elapsed;
				if ( gap >= 10 ) {
//					Thread.sleep(gap);
					if ( !isRunning() ) {
						return;
					}
				}
				
				m_jdbc.executeUpdate(m_insertSql, pstmt -> {
					for ( int i =0; i < columns.size(); ++i ) {
						pstmt.setObject(i+1, columns.get(i));
					}
					pstmt.execute();
					progress.step();
				});
			}
		}
	}
	
	private List<Object> buildJdbcColumns(NamedCsvRecord rec) throws SQLException {
		return FStream.from(m_tableInfo.getColumnInfoList())
						.zipWith(FStream.from(rec.getFields()))
						.mapOrThrow(pair -> {
							ColumnInfo colInfo = pair._1;
							String valStr = pair._2;
				
							Object value = colInfo.getMdtType().parseValueString(valStr);
							return colInfo.getMdtType().toJdbcObject(value);
						})
						.toList();
	}
}
