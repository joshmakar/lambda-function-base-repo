import { renderFilenameTimestamp } from './DateHandler';

/**
 * Generate an S3 key for a file in the format of 'YYYY/MM/DD/file_name_YYYY-MM-DD_HHMMSS_#####.ext'
 * @param filename The name of the file
 * @param extension The extension of the file, e.g. 'csv'
 * @param {Object} options - Additional options to pass.
 * @param {string} options.prependToPath - Prepend a string to the S3 key.
 * @returns An S3 key name
 */
export const generateS3Key = (filename: string, extension: string, { prependToPath = '' }: { prependToPath?: string } = {}): string => {
  const safeFilename = filename.replace(/[^a-zA-Z0-9]/g, '_');
  const safePrependToPath = !prependToPath ? '' : prependToPath.replace(/[^a-zA-Z0-9][\/]/g, '_').replace(/\/?$/, '/');
  const randomNumber = Math.floor(Math.random() * 90000) + 10000;
  const date = new Date();
  const key = `${safePrependToPath}${date.getFullYear()}/${date.getMonth() + 1}/${date.getDate()}/${safeFilename}_${renderFilenameTimestamp(date)}_${randomNumber}.${extension}`;

  return key;
}
