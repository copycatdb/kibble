export interface QueryResult {
  rows: Array<Record<string, any>>
  columns: Array<ColumnInfo>
  rowCount: number
}

export interface ColumnInfo {
  name: string
  type: string
}

export declare class Client {
  constructor(connectionString: string)
  connect(): Promise<void>
  query(sql: string, params?: any[]): Promise<QueryResult>
  execute(sql: string, params?: any[]): Promise<number>
  close(): Promise<void>
  end(): Promise<void>
}
