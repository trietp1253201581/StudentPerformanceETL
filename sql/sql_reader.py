import os

class NotSupportedQueryException(Exception):
    def __init__(self, message: str = "This query is not supported!"):
        super().__init__(message)

class SQLFileReader:
    def __init__(self):
        self._stmts = {}
    
    def read(self, sql_file_path: str) -> None:
        self.clear()
        curr_stmt = None
        sql_commands = []
        
        try:
            with open(sql_file_path, 'r') as file:
                for line in file:
                    line = line.strip()
                    #Responsiblity Comment
                    if line.startswith("--"):             
                        if curr_stmt is not None and sql_commands:
                            sql_str_commands = ' '.join(sql_commands).strip()
                            self._stmts[curr_stmt] = sql_str_commands
                        curr_stmt = line.replace('--', '').strip()
                        sql_commands = []
                    else:
                        if line:
                            sql_commands.append(line)
                if curr_stmt is not None and sql_commands:
                    sql_str_commands = ' '.join(sql_commands).strip()
                    self._stmts[curr_stmt] = sql_str_commands
        except Exception as ioException:
            raise ioException
    
    def get_enable_queries(self) -> list:
        return list(self._stmts.keys())
    
    def get_query_of(self, query_type: str):
        if query_type not in self.get_enable_queries():
            raise NotSupportedQueryException()
        return self._stmts[query_type]
    
    def clear(self) -> None:
        self._stmts.clear()
        
def unit_check() -> None:
    sqlFileReader = SQLFileReader()
    try:
        curr_dir = os.path.dirname(os.path.abspath(__file__))
        sql_file_path = os.path.join(curr_dir, 'queries.sql')
        sqlFileReader.read(sql_file_path=sql_file_path)
        print(sqlFileReader.get_enable_queries())
        print(sqlFileReader.get_query_of('SELECT1'))
    except Exception as ioException:
        print(ioException)

if __name__ == '__main__':
    unit_check()
    
    