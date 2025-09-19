/**
 * Другой алгоритм — находит максимум подмассива (Kadane).
 * Должен иметь низкую похожесть с двумя-sum реализациями.
 */
function maxSubArray(nums) {
  let maxSoFar = nums[0];
  let cur = nums[0];
  for (let i = 1; i < nums.length; i++) {
    cur = Math.max(nums[i], cur + nums[i]);
    maxSoFar = Math.max(maxSoFar, cur);
  }
  return maxSoFar;
}

// console.log(maxSubArray([-2,1,-3,4,-1,2,1,-5,4])); // 6
