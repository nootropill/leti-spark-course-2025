/**
 * Two Sum variant: переименованы переменные, другая стилизация.
 */
function twoSumVariant(arr, tgt) {
  const idxByValue = new Map();
  for (let idx = 0; idx < arr.length; idx++) {
    const complement = tgt - arr[idx];
    if (idxByValue.has(complement)) {
      return [idxByValue.get(complement), idx];
    }
    idxByValue.set(arr[idx], idx);
  }
  return null;
}

// console.log(twoSumVariant([2,7,11,15], 9)); // [0,1]
