#include <iostream>
#include <libpq-fe.h>
#include <libpq-events.h>
#include <sstream>

struct XLogRecPtr
{
	unsigned int		xlogid;			/* log file #, 0 based */
	unsigned int		xrecoff;		/* byte offset of location in log file */
public:
	const char* toHexString() const{
		char lsn[100];
		snprintf(lsn, sizeof(lsn), "%X/%X", (unsigned int)(xlogid), (unsigned int)xrecoff);
		return lsn;
	}
};

void identifySystem(PGconn* conn)
{

	try
	{
		PGresult* res = PQexec(conn, "IDENTIFY_SYSTEM");

		if (PQresultStatus(res) != PGRES_TUPLES_OK) {

			printf("No data retrieved\n");
			PQclear(res);
			return;
		}

		std::cout << std::endl;
		std::cout << "IDENTIFY_SYSTEM result is " << std::endl;
		std::cout << "systemid(text)  ";
		std::cout << PQgetvalue(res, 0, 0) << std::endl;
		std::cout << "timeline(int4)  ";
		std::cout << PQgetvalue(res, 0, 1) << std::endl;
		std::cout << "xlogpos(text)  ";
		std::cout << PQgetvalue(res, 0, 2) << std::endl;
		std::cout << "dbname(text)  ";
		std::cout << PQgetvalue(res, 0, 3) << std::endl;
		std::cout <<  std::endl;
		PQclear(res);

	}
	catch (const std::exception& e)
	{
		std::cerr << e.what() << std::endl;
	}

}

void readReplication(PGconn* conn, const std::string& replicationName)
{

	try
	{
		char cmd[256];
		snprintf(cmd, sizeof(cmd), "READ_REPLICATION_SLOT  \"%s\"", replicationName.c_str());
		PGresult* res = PQexec(conn, cmd);

		if (PQresultStatus(res) != PGRES_TUPLES_OK) {

			printf("No data retrieved\n");
			PQclear(res);
			return;
		}
		std::cout << std::endl;
		std::cout << "READ_REPLICATION_SLOT result " << std::endl;
		std::cout << "slot_type(text)  ";
		std::cout << PQgetvalue(res, 0, 0) << std::endl;
		std::cout << "restart_lsn(text)  ";
		std::cout << PQgetvalue(res, 0, 1) << std::endl;
		std::cout << "restart_tli(int8)  ";
		std::cout << PQgetvalue(res, 0, 2) << std::endl;
		std::cout << std::endl;
		PQclear(res);

	}
	catch (const std::exception& e)
	{
		std::cerr << e.what() << std::endl;
	}

}
void createSlot(PGconn* conn)
{

	try
	{
		PGresult* res = PQexec(conn, "CREATE_REPLICATION_SLOT my_physical_slot PHYSICAL  ");
		const ExecStatusType status = PQresultStatus(res);
		if (PQresultStatus(res) != PGRES_TUPLES_OK) {

			std::cerr << "could not create slot: " << PQerrorMessage(conn) << std::endl;

		}
		PQclear(res);

	}
	catch (const std::exception& e)
	{
		std::cerr << e.what() << std::endl;
	}

}
void timelineHistory(PGconn* conn)
{

	try
	{
		PGresult* res = PQexec(conn, "TIMELINE_HISTORY 1  ");
		const ExecStatusType status = PQresultStatus(res);
		if (PQresultStatus(res) != PGRES_TUPLES_OK) {

			std::cerr << "could not TIMELINE_HISTORY: " << PQerrorMessage(conn) << std::endl;
			PQclear(res);
			return;

		}
		std::cout << std::endl;
		std::cout << "TIMELINE_HISTORY result " << std::endl;
		std::cout << "filename   ";
		std::cout << PQgetvalue(res, 0, 0) << std::endl;
		std::cout << "content   ";
		std::cout << PQgetvalue(res, 0, 1) << std::endl;
		std::cout << std::endl;
		PQclear(res);
	}
	catch (const std::exception& e)
	{
		std::cerr << e.what() << std::endl;
	}

}

static PGresult* libpqrcv_PQexec(PGconn* conn,const char* query)
{
	PGresult* result = NULL;
	PGresult* lastResult = NULL;

	/*
	 * PQexec() silently discards any prior query results on the connection.
	 * This is not required for walreceiver since it's expected that walsender
	 * won't generate any such junk results.
	 */
	 /*
	  * Submit a query. Since we don't use non-blocking mode, this also can
	  * block. But its risk is relatively small, so we ignore that for now.
	  */
	if (!PQsendQuery(conn, query)) {
		return NULL;
	}

	for (;;) {
		/*
		 * Receive data until PQgetResult is ready to get the result without
		 * blocking.
		 */
		while (PQisBusy(conn)) {
			///CHECK_FOR_INTERRUPTS();
			int rr = 0;
			/*
			 * We don't need to break down the sleep into smaller increments,
			 * and check for interrupts after each nap, since we can just
			 * elog(FATAL) within SIGTERM signal handler if the signal arrives
			 * in the middle of establishment of replication connection.
			 */
			//if (!libpq_select(-1)) {
			//	pqClearAsyncResult(conn);
			//	continue; /* interrupted */
			//}
			if (PQconsumeInput(conn) == 0) {
			//	pqClearAsyncResult(conn);

				PQclear(lastResult);
				return NULL; /* trouble */
			}
		}

		/*
		 * Emulate the PQexec()'s behavior of returning the last result when
		 * there are many. Since walsender will never generate multiple
		 * results, we skip the concatenation of error messages.
		 */
		result = PQgetResult(conn);
		if (result == NULL) {
			break; /* query is complete */
		}

		PQclear(lastResult);
		lastResult = result;
		auto resultState = PQresultStatus(lastResult);
		if (PQresultStatus(lastResult) == PGRES_COPY_IN || PQresultStatus(lastResult) == PGRES_COPY_OUT ||
			PQresultStatus(lastResult) == PGRES_COPY_BOTH ||
			PQstatus(conn) == CONNECTION_BAD) {
			break;
		}
	}

	return lastResult;
}

void startReplication(PGconn* conn,
	const std::string& replicationName, 
	const XLogRecPtr& startpoint,
	const bool isLogical,
	long timeline = 0,
	const std::string& plugin = "")
{
	try
	{
		std::ostringstream cmd;
		cmd<<"START_REPLICATION ";
		if (!replicationName.empty()) {
			cmd <<"SLOT " << replicationName;
		}
		if (isLogical) {
			cmd <<" LOGICAL ";
		}
	    
		cmd << std::string(startpoint.toHexString());

		if (!isLogical && timeline > 0) {
			cmd << " TIMELINE " << timeline;

		}
		if (isLogical && !plugin.empty()) {
			//cmd << " ( \"plugin\" '" << plugin << "' )";
		//	cmd << " '" << plugin << "' ";
		}

		PGresult* res = libpqrcv_PQexec(conn, cmd.str().c_str());
		const ExecStatusType status = PQresultStatus(res);

        if (status != PGRES_COPY_BOTH)
		{
			std::cerr << "could not start WAL streaming: " << PQerrorMessage(conn) << std::endl;
			PQclear(res);
			return;
		}
		PQclear(res);

	}
	catch (const std::exception& e)	{

		std::cerr << e.what() << std::endl;
		return;
	}
	PGresult* pResult = NULL;
	char* szCopyStr = NULL;
	char* errormsg = NULL;
	char* buffer = NULL;               // for retrieving the data
	int nLen;                            // length of returned data


	/* Note we are not asynchronous */
	while ((nLen = PQgetCopyData(conn, &buffer, 0)) > 0) {
		printf("PQgetCopyData: read %d bytes, %s\n", nLen, buffer);
	}
	if (nLen == -2) {
		fprintf(stderr, "%s[%d]: PQgetCopyData error, %s\n",
			__FILE__, __LINE__, PQerrorMessage(conn));
	}

	if (buffer != NULL) {
		PQfreemem(buffer);
	}


}


int main(const char args[])
{
	std::setlocale(LC_ALL, "en_US.UTF-8");
	const std::string connectionString("replication=database host=127.0.0.1 port=5432 dbname=postgres  user=postgres password=84218421");
	//const std::string connectionString("replication=database host=127.0.0.1 port=5432 dbname=postgres  user=repuser password=replication");
	
	int lib_ver = PQlibVersion();

	printf("Version of libpq: %d\n", lib_ver);


	PGconn* conn = PQconnectdb(connectionString.c_str());

	if (PQstatus(conn) == CONNECTION_BAD) {

		fprintf(stderr, "Connection to database failed: %s\n",
			PQerrorMessage(conn));

	}
	//identifySystem(conn);
	//createSlot(conn);
	//readReplication(conn, std::string("my_physical_slot"));
	//timelineHistory(conn);
	XLogRecPtr startpoint;
	//startpoint.xlogid = 0; //0/174C888
	//startpoint.xrecoff = 0x174F288; // 0x174C888;

	//logical 
	//startReplication(conn, std::string("regression_slot"), startpoint, true);

	//wal2json

	startpoint.xlogid = 0;
	startpoint.xrecoff = 0x1752670;
	startReplication(conn, std::string("myslot"), startpoint, true, 0, "wal2json");

	//physical
	//startReplication(conn, std::string(""), startpoint, false);

	return 0;
}