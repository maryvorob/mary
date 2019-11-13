WITH
  completions AS (
    SELECT
      student_id,
      course_id,
      -- первый раз, когда студент закончил этот курс
      min(timest) AS cmpl_ts,
      -- первый раз, когда студент закончил какой-либо курс
      min(min(timest)) OVER (PARTITION BY student_id) AS stud_cmpl_ts
    FROM lessons
    GROUP BY 1, 2
  ),
  payments AS (
    SELECT
      student_id,
      course_id,
      -- первый раз, когда студент купил этот курс
      min(timest) AS paid_ts,
      -- первый раз, когда студент купил какой-либо курс
      min(min(timest)) OVER (PARTITION BY student_id) AS stud_paid_ts
    FROM payments
    GROUP BY 1, 2
  ),
  courses AS (
    SELECT *,
      -- Закончил этот и потои купил любой курс
      (cmpl_ts < stud_paid_ts) :: INT AS cmpl_paid_any,
      -- Закончил и потом купил этот же курс
      (cmpl_ts < paid_ts) :: INT AS cmpl_paid,
      -- Закончил и купил этот же курс в один день (не важно что было раньше)
      (cmpl_ts :: DATE = paid_ts :: DATE) :: INT AS same_day,
      -- Закончил это курс до первой покупки
      (cmpl_ts < coalesce(stud_paid_ts, '3000-01-01')) :: INT AS cmpl
    FROM completions
    LEFT JOIN payments
    USING (student_id, course_id)
  ),
  agg AS (
    SELECT
      -- Количество законченных курсов, после которых пользователи что-то купили
      sum(cmpl_paid_any) AS cmpl_paid_any,
      -- Количество законченных курсов, которые потом купили
      sum(cmpl_paid) AS cmpl_paid,
      -- Количество законченных курсов, которые потом купили в тот же день
      sum(cmpl_paid * same_day) AS cmpl_paid_sd,
      -- количество курсов, законченных пользователями, которые раньше не покупали
      sum(cmpl) AS cmpl
    FROM courses
  )
SELECT
  -- Общая конверсия из решения курса в покупку
  cmpl_paid_any / cmpl AS conv_cmpl_paid_any,
  -- Покупки того же курса, который решали перед этим
  cmpl_paid / cmpl AS conv_cmpl_paid,
  -- Покупки этого же курса в этот же день
  cmpl_paid_sd AS cmpl_paid_sd
FROM agg
