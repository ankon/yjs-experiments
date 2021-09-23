import { DynamoDB } from 'aws-sdk';
import { nanoid } from 'nanoid';
import * as Y from 'yjs';

export interface Storage {
	storeUpdate(id: string, update: Uint8Array): Promise<void>;
	getStateUpdate(id: string, knownState?: Uint8Array): Promise<Uint8Array>;
}

// #region Memory Storage
export function createMemoryStorage(): Storage {
	const serverKnownUpdates: Map<string, Uint8Array[]> = new Map();

	return {
		async storeUpdate(id, update) {
			let updates = serverKnownUpdates.get(id);
			if (!updates) {
				updates = [];
				serverKnownUpdates.set(id, updates);
			}
			updates.push(update);
		},
		async getStateUpdate(id, knownState) {
			const updates = serverKnownUpdates.get(id);
			if (!updates) {
				throw new Error('Unknown document');
			}
			const update = Y.mergeUpdates(updates);
			if (knownState) {
				return Y.diffUpdate(update, knownState);
			} else {
				return update;
			}
		},
	};
}
// #endregion

// Expects DynamoDB available (npx dynalite --path data)
export async function createDynamoDBStorage(endpoint = 'http://localhost:4567'): Promise<Storage> {
	// TODO: Need a storage for "latest generation" of each document, which in carrot-graphql would be part
	//       of the reference in the ObjectTable
	function getLatestGeneration(_id: string) {
		return 1;
	}

	/** Get the id for all updates of the document with the given generation */
	function getDynamoDBId(documentId: string, generation = getLatestGeneration(documentId)): string {
		return `${documentId}:${generation}`;
	}

	const tableName = 'updates';
	const dynamo = new DynamoDB({ endpoint, region: process.env.AWS_REGION || 'local' })

	do {
		try {
			const results = await dynamo.describeTable({
				TableName: tableName,
			}).promise();
			console.log(`Table ${tableName}: ${results.Table.TableStatus}`);
			if (results.Table.TableStatus === 'ACTIVE') {
				break;
			} else {
				await new Promise(resolve => setTimeout(resolve, 1000));
			}
		} catch (err) {
			await dynamo.createTable({
				TableName: tableName,
				BillingMode: 'PAY_PER_REQUEST',
				AttributeDefinitions: [
					/** Id of the document */
					{ AttributeName: 'id', AttributeType: 'S' },
					/** Timestamp of the update */
					{ AttributeName: 'ts', AttributeType: 'N' },
					// Non-key: { AttributeName: 'update', AttributeType: 'B' }
				],
				KeySchema: [
					{ AttributeName: 'id', KeyType: 'HASH' },
					{ AttributeName: 'ts', KeyType: 'RANGE' }
				]
			}).promise();
		}
	} while (true);

	return {
		async storeUpdate(id, update) {
			// Limit retries at one point? Exp back-off?
			const RETRIES = 5;
			for (let i = 0; i < RETRIES; i++) {
				const ts = Date.now();
				try {
					await dynamo.putItem({
						TableName: tableName,
						Item: {
							id: { S: getDynamoDBId(id) },
							ts: { N: String(ts) },
							update: {
								B: Buffer.from(update).toString('base64'),
							},
						},
						ConditionExpression: 'attribute_not_exists(ts)',
					}).promise();

					return;
				} catch (err) {
					console.warn(`Conflict for ${id}@${ts}: ${err.message}`, err);
				}
			}

			throw new Error(`Update for ${id} not stored`);
		},
		async getStateUpdate(id, knownState) {
			// Walk over all updates, and merge each page into the result
			const pageUpdates = [];
			let startKey;
			do {
				const { LastEvaluatedKey: nextKey, Items: items } = await dynamo.query({
					TableName: tableName,
					KeyConditionExpression: 'id = :id',
					ProjectionExpression: '#update',
					ExpressionAttributeValues: {
						':id': { S: getDynamoDBId(id) },
					},
					ExpressionAttributeNames: {
						'#update': 'update',
					},
					ExclusiveStartKey: startKey,
				}).promise();
				startKey = nextKey;

				// XXX: If no items are found we're going to produce a single update, which would create an empty document.

				// Merge all found updates into a single page update
				const { updates, length } = items.reduce((r, item) => {
					const update = Buffer.from(String(item.update.B), 'base64');
					r.length += update.length;
					r.updates.push(update);
					return r;
				}, { updates: [] as Buffer[], length: 0 });
				const pageUpdate = Y.mergeUpdates(updates);
				pageUpdates.push(pageUpdate);
				console.log(`Merged ${items.length} updates (size = ${length}) into 1 page update (size = ${pageUpdate.byteLength})`);
			} while (startKey);

			// Merge all page updates
			let update;
			if (pageUpdates.length === 1) {
				update = pageUpdates[0];
			} else {
				update = Y.mergeUpdates(pageUpdates);
				console.log(`Merged ${pageUpdates.length} page updates (size = ${pageUpdates.reduce((r, u) => r + u.byteLength, 0)}) into 1 (size = ${update.length})`);
			}

			let result = knownState ? Y.diffUpdate(update, knownState) : update;
			console.log(`Final state: ${result.length} bytes`);
			return result;
		},
	}
}

function makeTestStorage<T extends Storage>(storage: T): T & { readonly allStored: Promise<void> } {
	const allUpdates: Promise<void>[] = [];
	return {
		...storage,
		storeUpdate(id, update) {
			const promise = storage.storeUpdate(id, update);
			allUpdates.push(promise);
			return promise;
		},
		get allStored() {
			return Promise.all(allUpdates).then(() => { /* Ignore results */ });
		}
	};
}

async function play(storage: Storage) {
	// For playing/testing: A wrapper to be able to simulate "all done"
	const testStorage = makeTestStorage(storage);

	/** GUID for a specific document */
	const DOCUMENT_1 = 'doc1';

	// Client makes up a document
	const ydoc1 = new Y.Doc({ guid: DOCUMENT_1 });
	console.log(`ydoc1: ${ydoc1.guid}`);
	ydoc1.on('update', update => testStorage.storeUpdate(ydoc1.guid, update));

	const map1 = ydoc1.getMap('experiment');
	for (let i = 0; i < 3000; i++) {
		map1.set('foo', nanoid(128));
		map1.set('other', i);
	}

	await testStorage.allStored;

	// Another client asks for the document from the server
	const ydoc2 = new Y.Doc({ guid: DOCUMENT_1 });
	console.log(`ydoc2: ${ydoc2.guid}`);
	const initialUpdate2 = await testStorage.getStateUpdate(ydoc2.guid);
	Y.applyUpdate(ydoc2, initialUpdate2, 'server');

	// Similar: On changes the client pushes the updates to the server
	ydoc2.on('update', update => testStorage.storeUpdate(ydoc2.guid, update));
	const map2 = ydoc2.getMap('experiment');
	map2.set('foo', 'baz');

	await testStorage.allStored;

	// Show current document state
	const ydoc3 = new Y.Doc({ guid: DOCUMENT_1 });
	console.log(`ydoc3: ${ydoc3.guid}`);
	const initialUpdate3 = await testStorage.getStateUpdate(DOCUMENT_1);
	Y.applyUpdate(ydoc3, initialUpdate3, 'server');

	const map3 = ydoc3.getMap('experiment');
	console.log(map3.toJSON());
}

async function main() {
	try {
		const memory = await createMemoryStorage();
		await play(memory);
	} catch (err) {
		console.error(`Failed for memory: ${err.message}`);
	}

	try {
		const ddb = await createDynamoDBStorage();
		await play(ddb);
	} catch (err) {
		console.error(`Failed for DynamoDB: ${err.message}`);
	}
}

main();
