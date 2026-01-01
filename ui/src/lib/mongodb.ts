import { MongoClient, Db } from 'mongodb';

const uri = process.env.MONGO_URI || 'mongodb://localhost:27017';
const dbName = process.env.MONGO_DB || 'claude_logs';

let cachedClient: MongoClient | null = null;
let cachedDb: Db | null = null;

export async function getDatabase(): Promise<Db> {
  if (cachedDb) {
    return cachedDb;
  }

  if (!cachedClient) {
    cachedClient = await MongoClient.connect(uri, {
      maxPoolSize: 10,
    });
  }

  cachedDb = cachedClient.db(dbName);
  return cachedDb;
}

export async function getConversationsCollection() {
  const db = await getDatabase();
  return db.collection('conversations');
}
