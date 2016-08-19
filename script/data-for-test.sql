INSERT INTO
plans(
  id,
  title,
  description,
  image,
  thumbnail,
  period
)
VALUES(
  '00000000-0000-0000-0000-000000000000',
  '主计划',
  '自驾车意外|驾乘通赔|互助1年',
  'https://www.baidu.com/img/bd_logo1.png',
  'https://www.baidu.com/img/bd_logo1.png',
  365
);

INSERT INTO
plan_rules(
  id,
  pid,
  name,
  title,
  description
)
VALUES(
  '00000000-0000-0000-0000-000000000001',
  '00000000-0000-0000-0000-000000000000',
  '互助人群',
  '拥有良好驾驶习惯的北京六环内行驶车辆',
  '<div>为保障整体计划参与者利益，将严格控制加入人群 <br/> <ul> <li>北京六环内行驶车辆</li> <li>最近2年未在保险公司出险</li> <li>从未发生过酒驾、闯红灯等严重交通违章行为</li> <li>驾驶人拥有2年以上驾龄或驾驶里程超过20000公里</li> </ul> </div>'
);

INSERT INTO
plan_rules(
  id,
  pid,
  name,
  title,
  description
)
VALUES(
  '00000000-0000-0000-0000-000000000002',
  '00000000-0000-0000-0000-000000000000',
  '互助范围',
  '机动车车辆损失、机动车车辆盗抢等保障',
  '<ul><li>机动车车辆损失</li><li>机动车车辆盗抢等保障</li></ul>'
);

INSERT INTO
plan_rules(
  id,
  pid,
  name,
  title,
  description
)
VALUES(
  '00000000-0000-0000-0000-000000000003',
  '00000000-0000-0000-0000-000000000000',
  '互助模式',
  '驾驶人与车辆绑定模式',
  '<ul><li>驾驶人</li><li>车辆绑定模式</li></ul>'
);

INSERT INTO
plan_rules(
  id,
  pid,
  name,
  title,
  description
)
VALUES(
  '00000000-0000-0000-0000-000000000004',
  '00000000-0000-0000-0000-000000000000',
  '互助期',
  '计划生效起365天内',
  '<ul><li>计划生效起365天内</li><li>计划生效起365天内</li></ul>'
);


