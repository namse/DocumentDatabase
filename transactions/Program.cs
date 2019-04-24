using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using Amazon.DynamoDBv2;
using MessagePack;
using StackExchange.Redis;

namespace transactions
{
    public enum OriginType
    {
        Steam,
    }

    [Union(0, typeof(IdentityDocument))]
    [Union(1, typeof(AccountDocument))]
    public interface IDocument
    {
        string Key { get; }
        long LockVersion { get; set; }
    }


    [MessagePackObject]
    public class IdentityDocument: IDocument
    {
        [Key(0)]
        public Guid Id;
        [Key(1)]
        public OriginType OriginType;
        [Key(2)]
        public Guid AccountId;
        [Key(3)]
        public long LockVersion { get; set; }

        [IgnoreMember]
        public string Key => $"identity.{Id}";
        public static string KeyOf(Guid id) => $"identity.{id}";

        public IdentityDocument(Guid id, OriginType originType, Guid accountId, long lockVersion = 0)
        {
            Id = id;
            OriginType = originType;
            AccountId = accountId;
            LockVersion = lockVersion;
        }
    }

    public class IdentityDocumentDatabase: DocumentDatabase<IdentityDocument>
    {
        protected override string TableName => "Identity";
        public IdentityDocumentDatabase(IDocumentDatabaseAdapter documentDatabaseAdapter) : base(documentDatabaseAdapter)
        {
        }
    }

    [MessagePackObject]
    public class AccountDocument : IDocument
    {
        [Key(0)]
        public Guid Id;
        [Key(1)]
        public long LockVersion { get; set; }

        [IgnoreMember]
        public string Key => KeyOf(Id);
        public static string KeyOf(Guid id) => $"account.{id}";

        public AccountDocument(Guid id, long lockVersion = 0)
        {
            Id = id;
            LockVersion = lockVersion;
        }
    }
    public class AccountDocumentDatabase : DocumentDatabase<AccountDocument>
    {
        protected override string TableName => "Identity";
        public AccountDocumentDatabase(IDocumentDatabaseAdapter documentDatabaseAdapter) : base(documentDatabaseAdapter)
        {
        }
    }

    public interface IDocumentDatabaseAdapter
    {
        Task<TDocument> GetAsync<TDocument>(string key) where TDocument : IDocument;
        Task<bool> SaveAsync<TDocument>(string key, TDocument value) where TDocument : IDocument;

        Task<bool> ExecuteTransaction(IEnumerable<TransactionRequest> transactionRequests);
        Task<TDocument> LockAndGetAsync<TDocument>(string key) where TDocument : IDocument;
    }

    public class RedisDocumentDatabaseAdapter : IDocumentDatabaseAdapter
    {
        private readonly IDatabase _redisDatabase;

        public RedisDocumentDatabaseAdapter(IDatabase redisDatabase)
        {
            _redisDatabase = redisDatabase;
        }

        private string GetLockKey(string key) => $"transaction-lock.{key}";

        public async Task<TDocument> GetAsync<TDocument>(string key) where TDocument : IDocument
        {
            var serialized = await _redisDatabase.StringGetAsync(key);
            return serialized.IsNull
                ? default
                : Deserialize<TDocument>(serialized);
        }

        public Task<bool> SaveAsync<TDocument>(string key, TDocument value) where TDocument : IDocument
        {
            var serialized = Serialize(value);
            return _redisDatabase.StringSetAsync(key, serialized);
        }

        public async Task<bool> ExecuteTransaction(IEnumerable<TransactionRequest> transactionRequests)
        {
            var transaction = _redisDatabase.CreateTransaction();

            foreach (var transactionRequest in transactionRequests)
            {
                var document = transactionRequest.Document;
                var key = document.Key;
                
                if (transactionRequest.KeyExisted)
                {
                    var lockKey = GetLockKey(key);
                    var lockVersion = document.LockVersion;
                    transaction.AddCondition(Condition.StringEqual(lockKey, lockVersion - 1));
                }
                else
                {
                    transaction.AddCondition(Condition.KeyNotExists(key));
                }

                var serialized = Serialize(document);
                _ = transaction.StringSetAsync(key, serialized);
            }

            return await transaction.ExecuteAsync();
        }

        public async Task<TDocument> LockAndGetAsync<TDocument>(string key) where TDocument : IDocument
        {
            var document = await GetAsync<TDocument>(key);

            if (document is null)
            {
                return default;
            }

            var lockKey = GetLockKey(key);
            await _redisDatabase.StringSetAsync(lockKey, document.LockVersion);

            return document;
        }

        public ITransaction CreateRedisTransaction()
        {
            return _redisDatabase.CreateTransaction();
        }

        public byte[] Serialize<T>(T value)
        {
            var serialized = MessagePackSerializer.Serialize(value);
            return serialized;
        }

        public T Deserialize<T>(byte[] serialized)
        {
            return MessagePackSerializer.Deserialize<T>(serialized);
        }
    }

    //public class DynamoDocumentDatabaseAdapter : IDocumentDatabaseAdapter
    //{
    //    private readonly AmazonDynamoDBClient _dynamoDbClient;

    //    public DynamoDocumentDatabaseAdapter(AmazonDynamoDBClient dynamoDbClient)
    //    {
    //        _dynamoDbClient = dynamoDbClient;
    //    }

    //    public Task<TDocument> GetAsync<TDocument>(string key) where TDocument : IDocument
    //    {
    //    }

    //    public Task<bool> SaveAsync<TDocument>(string key, TDocument value) where TDocument : IDocument
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public BaseDocumentDatabaseTransaction StartTransaction()
    //    {
    //        throw new NotImplementedException();
    //    }
    //}
    
    public abstract class DocumentDatabase<TDocument> where TDocument : IDocument
    {
        public readonly IDocumentDatabaseAdapter DocumentDatabaseAdapter;
        protected abstract string TableName { get; }

        protected DocumentDatabase(IDocumentDatabaseAdapter documentDatabaseAdapter)
        {
            DocumentDatabaseAdapter = documentDatabaseAdapter;
        }

        public Task<TDocument> GetAsync(string key)
        {
            return DocumentDatabaseAdapter.GetAsync<TDocument>(key);
        }

        public Task<bool> SaveAsync(string key, TDocument value)
        {
            return DocumentDatabaseAdapter.SaveAsync<TDocument>(key, value);
        }
    }

    public interface ITransactionDocumentDatabase
    {
        List<TransactionRequest> TransactionRequests { get; }
    }

    public readonly struct TransactionRequest
    {
        public readonly bool KeyExisted;
        public readonly IDocument Document;

        public TransactionRequest(bool keyExisted, IDocument document)
        {
            Document = document;
            KeyExisted = keyExisted;
        }
    }

    public class TransactionDocumentDatabase<TDocument>: ITransactionDocumentDatabase where TDocument: IDocument
    {
        private readonly IDocumentDatabaseAdapter _documentDatabaseAdapter;
        private readonly Dictionary<string, long> _lockVersionDictionary = new Dictionary<string, long>();
        public List<TransactionRequest> TransactionRequests { get; } = new List<TransactionRequest>();

        public TransactionDocumentDatabase(IDocumentDatabaseAdapter documentDatabaseAdapter)
        {
            _documentDatabaseAdapter = documentDatabaseAdapter;
        }

        public async Task<TDocument> LockAsync(string key)
        {
            var document = await _documentDatabaseAdapter.LockAndGetAsync<TDocument>(key);

            if (!(document is null))
            {
                _lockVersionDictionary.Add(key, document.LockVersion);
            }

            return document;
        }

        public void QueueSave(string key, TDocument document)
        {
            if (_lockVersionDictionary.TryGetValue(key, out var lockVersion))
            {
                document.LockVersion = lockVersion + 1;
                TransactionRequests.Add(new TransactionRequest(true, document));
            }
            else
            {
                document.LockVersion = 0;
                TransactionRequests.Add(new TransactionRequest(false, document));
            }
        }
    }

    public sealed class DocumentDatabaseTransaction
    {
        private IDocumentDatabaseAdapter _documentDatabaseAdapter;
        private readonly List<ITransactionDocumentDatabase> _transactionDocumentDatabases = new List<ITransactionDocumentDatabase>();

        public TransactionDocumentDatabase<TDocument> AddTransactionDocumentDatabase<TDocument>(DocumentDatabase<TDocument> documentDatabase) where TDocument: IDocument
        {
            if (_documentDatabaseAdapter is null)
            {
                _documentDatabaseAdapter = documentDatabase.DocumentDatabaseAdapter;
            }
            else if (_documentDatabaseAdapter.GetType() != documentDatabase.DocumentDatabaseAdapter.GetType())
            {
                throw new Exception("database adapter type must be same");
            }

            var transactionDocumentDatabase =new TransactionDocumentDatabase<TDocument>(_documentDatabaseAdapter);
            _transactionDocumentDatabases.Add(transactionDocumentDatabase);

            return transactionDocumentDatabase;
        }

        public Task<bool> ExecuteAsync()
        {
            var transactionRequests = _transactionDocumentDatabases.SelectMany(transactionDocumentDatabase =>
                transactionDocumentDatabase.TransactionRequests);

            return _documentDatabaseAdapter.ExecuteTransaction(transactionRequests);
        }
    }

    class Program
    {
        static async Task Main(string[] args)
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var db = redis.GetDatabase();


            var accountId = Guid.NewGuid();
            var identityId = Guid.NewGuid();

            Console.WriteLine($"accountId: {accountId}");
            Console.WriteLine($"identityId: {identityId}");

            {
                var identityDocumentDatabase = new IdentityDocumentDatabase(new RedisDocumentDatabaseAdapter(db));
                var identity = new IdentityDocument(identityId, OriginType.Steam, Guid.Empty);
                await identityDocumentDatabase.SaveAsync(identity.Key, identity);
            }

            {
                var transaction = new DocumentDatabaseTransaction();

                var accountTransaction = transaction.AddTransactionDocumentDatabase(
                    new AccountDocumentDatabase(new RedisDocumentDatabaseAdapter(db)));

                var accountKey = AccountDocument.KeyOf(accountId);
                var account = await accountTransaction.LockAsync(accountKey);
                if (!(account is null))
                {
                    throw new Exception("Account already exists");
                }

                var identityTransaction = transaction.AddTransactionDocumentDatabase(
                    new IdentityDocumentDatabase(new RedisDocumentDatabaseAdapter(db)));

                var identityKey = IdentityDocument.KeyOf(identityId);

                var identity = await identityTransaction.LockAsync(identityKey);
                identity.AccountId = accountId;
                identityTransaction.QueueSave(identityKey, identity);

                account = new AccountDocument(accountId);
                accountTransaction.QueueSave(accountKey, account);

                var isTransactionSuccessful = await transaction.ExecuteAsync();
                Console.WriteLine(isTransactionSuccessful);
            }
        }

        static async Task SaveAccountAsync(IDatabase redisDatabase, AccountDocument accountDocument)
        {
            var serializedAccount = MessagePackSerializer.Serialize(accountDocument);

            await redisDatabase.StringSetAsync(accountDocument.Key, serializedAccount);
        }
    }
}
