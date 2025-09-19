/**
 * Two Sum, используя forEach. Логика та же, синтаксис другой.
 */
function twoSumForEach(nums, target) {
  const map = new Map();
  let result = null;
  nums.forEach((value, index) => {
    if (result !== null) return;
    const need = target - value;
    if (map.has(need)) {
      result = [map.get(need), index];
      return;
    }
    map.set(value, index);
  });
  return result;
}

// console.log(twoSumForEach([2,7,11,15], 9));
