/**
 * Two Sum с выделением вспомогательной функции.
 */
function findComplementIndex(map, value) {
  return map.has(value) ? map.get(value) : -1;
}

function twoSumReordered(nums, target) {
  const valueToIndex = new Map();
  for (let i = 0; i < nums.length; i++) {
    const needed = target - nums[i];
    const found = findComplementIndex(valueToIndex, needed);
    if (found !== -1) {
      return [found, i];
    }
    valueToIndex.set(nums[i], i);
  }
  return null;
}

// console.log(twoSumReordered([2,7,11,15], 9));
