/**
 * Generate the datetime to UTC Y-m-d H:i:s format
 * @param {Date} date The date to be converted
 * @returns {string} UTC in the format of Y-m-d H:i:s
 */
export function toUTCDateTimeString(date: Date): string {
  const p = new Intl.DateTimeFormat('en', {
    year:'numeric',
    month:'2-digit',
    day:'2-digit',
    hour:'2-digit',
    minute:'2-digit',
    second:'2-digit',
    hourCycle: 'h23',
  }).formatToParts(date).reduce((acc, part) => {
    acc[part.type] = part.value;
    return acc;
  }, { year: '', month: '', day: '', hour: '', minute: '', second: '', hourCycle: '', dayPeriod: '', era: '', literal: '', timeZoneName: '', weekday: '' });

  return `${p.year}-${p.month}-${p.day} ${p.hour}:${p.minute}:${p.second}`;
}

/**
 * Render a datetime to UTC YYYY-MM-DD_HHMMSS format
 * @param {Date} date The date to be converted
 * @returns {string} UTC in the format of Y-m-d_His
 */
export function renderFilenameTimestamp(date: Date = new Date): string {
  const p = new Intl.DateTimeFormat('en', {
    year:'numeric',
    month:'2-digit',
    day:'2-digit',
    hour:'2-digit',
    minute:'2-digit',
    second:'2-digit',
    hourCycle: 'h23',
  }).formatToParts(date).reduce((acc, part) => {
    acc[part.type] = part.value;
    return acc;
  }, { year: '', month: '', day: '', hour: '', minute: '', second: '', hourCycle: '', dayPeriod: '', era: '', literal: '', timeZoneName: '', weekday: '' });

  return `${p.year}-${p.month}-${p.day}_${p.hour}${p.minute}${p.second}`;
}
