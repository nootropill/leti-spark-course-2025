/**
 * Two Sum: возвращает индексы двух чисел, которые в сумме дают target.
 * Используется словарь (map) для O(n).
 */
function twoSum(nums, target) {
  const map = new Map();
  for (let i = 0; i < nums.length; i++) {
    const need = target - nums[i];
    if (map.has(need)) {
      return [map.get(need), i];
    }
    map.set(nums[i], i);
  }
  return null;
}

// Пример
// console.log(twoSum([2,7,11,15], 9)); // [0,1]
