package cn.edu.tsinghua.iotdb.engine.lru;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeProcessor;
import cn.edu.tsinghua.iotdb.exception.LRUManagerException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;

/**
 * <p>
 * LRUManager manage a list of processor: {@link FileNodeProcessor}
 * processor<br>
 *
 * @author kangrong
 * @author liukun
 */
public abstract class LRUManager<T extends LRUProcessor> {
	private static final Logger LOGGER = LoggerFactory.getLogger(LRUManager.class);

	private static final long removeCheckInterval = 100;
	// The insecure variable in multiple thread
	private Map<String, T> processorMap;

	protected final String normalDataDir;

	protected LRUManager(int maxLRUNumber, String normalDataDir) {
		LOGGER.info("The max of LRUProcessor number is {}", maxLRUNumber);
		processorMap = new ConcurrentHashMap<String, T>();

		if (normalDataDir.charAt(normalDataDir.length() - 1) != File.separatorChar)
			normalDataDir += File.separatorChar;
		this.normalDataDir = normalDataDir;
		File dir = new File(normalDataDir);
		if (dir.mkdirs()) {
			LOGGER.info("{} dir home is not exists, create it", this.getClass().getSimpleName());
		}
	}

	/**
	 * <p>
	 * Get the data directory
	 * 
	 * @return data directory
	 */
	public String getNormalDataDir() {
		return normalDataDir;
	}

	/**
	 * check the processor exists or not by namespacepath
	 * 
	 * @param namespacePath
	 * @return
	 */
	public boolean containNamespacePath(String namespacePath) {
		return processorMap.containsKey(namespacePath);
	}

	/**
	 * <p>
	 * Get processor just using the metadata path
	 * 
	 * @param path
	 * @param isWriteLock
	 * @return LRUProcessor
	 * @throws ProcessorException
	 * @throws IOException
	 */
	public T getProcessorWithDeltaObjectIdByLRU(String path, boolean isWriteLock) throws LRUManagerException {
		return getProcessorWithDeltaObjectIdByLRU(path, isWriteLock, null);
	}

	/**
	 * check whether the given path's exists. If it doesn't exist, add it to
	 * list. If the list has been full(list.size == maxLRUNodeNumber), remove
	 * the last one. If it exists, raise this processor to the first position
	 *
	 * @param path
	 *            - measurement path in name space.
	 * @param isWriteLock
	 * @param parameters
	 *            - save some key-value information used for constructing a
	 *            special processor
	 * @return return processor which has the specified nsPath
	 * @throws ProcessorException
	 * @throws IOException
	 */
	public T getProcessorWithDeltaObjectIdByLRU(String path, boolean isWriteLock, Map<String, Object> parameters)
			throws LRUManagerException {
		String nsPath;
		try {
			// ensure thread securely by ZJR
			nsPath = MManager.getInstance().getFileNameByPath(path);
		} catch (PathErrorException e) {
			LOGGER.error("MManager get nameSpacePath error, path is {}", path);
			throw new LRUManagerException(e);
		}
		return getProcessorByLRU(nsPath, isWriteLock, parameters);
	}

	/**
	 * Get processor using namespacepath
	 * 
	 * @param namespacePath
	 * @param isWriteLock
	 *            true if add a write lock on return {@code T}
	 * @return
	 * @throws ProcessorException
	 * @throws IOException
	 */
	public T getProcessorByLRU(String namespacePath, boolean isWriteLock) throws LRUManagerException {
		return getProcessorByLRU(namespacePath, isWriteLock, null);
	}

	/**
	 * Get the Processor from memory or construct one new processor by the
	 * nameSpacePath</br>
	 * 
	 * check whether the given namespace path exists. If it doesn't exist, add
	 * it to map.
	 * 
	 * @param namespacePath
	 * @param isWriteLock
	 * @param parameters
	 * @return
	 * @throws LRUManagerException
	 */
	public T getProcessorByLRU(String namespacePath, boolean isWriteLock, Map<String, Object> parameters)
			throws LRUManagerException {

		T processor = null;
		processor = processorMap.get(namespacePath);
		if (processor != null) {
			processor.lock(isWriteLock);
		} else {
			namespacePath = namespacePath.intern();
			// calculate the value with same key synchronously
			synchronized (namespacePath) {
				if (processorMap.containsKey(namespacePath)) {
					processor = processorMap.get(namespacePath);
					processor.lock(isWriteLock);
				} else {
					// calculate the value with the key monitor
					LOGGER.debug("Calcuate the processor, the nameSpacePath is {}, Thread is {}", namespacePath,
							Thread.currentThread().getId());
					processor = constructNewProcessor(namespacePath);
					processorMap.put(namespacePath, processor);
					processor.lock(isWriteLock);
				}
			}
		}
		return processor;
	}

	/**
	 * Try to close the special processor whose nameSpacePath is nsPath Notice:
	 * this function may not close the special processor
	 * 
	 * @param nsPath
	 * @throws LRUManagerException
	 */
	private void close(String nsPath, Iterator<Entry<String, T>> processorIterator) throws LRUManagerException {
		if (processorMap.containsKey(nsPath)) {
			LRUProcessor processor = processorMap.get(nsPath);
			if (processor.tryWriteLock()) {
				try {
					if (processor.canBeClosed()) {
						try {
							LOGGER.info("Close the processor, the nameSpacePath is {}", nsPath);
							processor.close();
							// TODO: whether remove the processor in the map or not
							processorMap.remove(nsPath);
						} catch (ProcessorException e) {
							LOGGER.error("Close processor error when close one processor, the nameSpacePath is {}",
									nsPath);
							throw new LRUManagerException(e);
						}
					} else {
						LOGGER.warn("The processor can't be closed, the nameSpacePath is {}", nsPath);
					}
				} finally {
					processor.writeUnlock();
				}
			} else {
				LOGGER.warn("Can't get the write lock the processor and close the processor, the nameSpacePath is {}",
						nsPath);
			}
		} else {
			LOGGER.warn("The processorMap does't contains the nameSpacePath {}", nsPath);
		}
	}

	/**
	 * Try to close all processors which can be closed now.
	 * 
	 * @return false - can't close all processors true - close all processors
	 * @throws LRUManagerException
	 */
	protected boolean closeAll() throws LRUManagerException {
		Iterator<Entry<String, T>> processorIterator = processorMap.entrySet().iterator();
		while (processorIterator.hasNext()) {
			Entry<String, T> processorEntry = processorIterator.next();
			try {
				close(processorEntry.getKey(), processorIterator);
			} catch (LRUManagerException e) {
				LOGGER.error("Close processor error when close all processors, the nameSpacePath is {}",
						processorEntry.getKey());
				throw e;
			}
		}
		return processorMap.isEmpty();
	}

	/**
	 * close one processor which is in the LRU list
	 * 
	 * @param namespacePath
	 * @return
	 * @throws LRUManagerException
	 */
	protected boolean closeOneProcessor(String namespacePath) throws LRUManagerException {
		if (processorMap.containsKey(namespacePath)) {
			LRUProcessor processor = processorMap.get(namespacePath);
			try {
				processor.writeLock();
				// wait until the processor can be closed
				while (!processor.canBeClosed()) {
					try {
						TimeUnit.MILLISECONDS.sleep(100);
					} catch (InterruptedException e) {
						LOGGER.warn("Interrupted when waitting to close one processor.");
					}
				}
				processor.close();
				processorMap.remove(namespacePath);
			} catch (ProcessorException e) {
				LOGGER.error("Close processor error when close one processor, the nameSpacePath is {}.", namespacePath);
				throw new LRUManagerException(e);
			} finally {
				processor.writeUnlock();
			}
		}
		return true;
	}

	/**
	 * <p>
	 * construct processor using namespacepath and key-value object<br>
	 * 
	 * @param namespacePath
	 * @param parameters
	 * @return
	 * @throws LRUManagerException
	 */
	protected abstract T constructNewProcessor(String namespacePath) throws LRUManagerException;
}
