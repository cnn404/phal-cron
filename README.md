
## Phalapi框架定时任务支持

#### phalapi框架配置定时任务比较麻烦，这个项目是对Phalapi框架做的定时任务功能补充，通过非常简单的三个步骤配置实现其他框架的定时任务功能。


### 接入步骤1：

```
.
├── ReadMe.md
├── bin
│   └── phalapi-cron
```
```php
#!/usr/bin/env php
<?php
require_once dirname(__FILE__) . '/../public/init.php';
use Coralme\PhalCron\Schedule;
class Cron extends Schedule
{
    public function schedule(Schedule $schedule)
    {
        #示例：
        $schedule->add('*/5 * * * *', 'Admin.DelAccount.autoDelAccount')->outPutLog('/var/www/html/runtime/autoDelAccount.log');
        $schedule->add('*/3 * * * *', 'Admin.DelAccount.autoDelAccount')->outPutLog('/var/www/html/runtime/autoDelAccount2.log');
        $schedule->add('0 10 * * *', 'Admin.DelAccount.autoDelAccount')->outPutLog('/var/www/html/runtime/autoDelAccount3.log');
    }
}

$cron = new Cron();
$cron->run();
```

### 接入步骤2：

我们只需要在 `Shedule` 方法里按照示例添加自己的定时任务即可

`add` 第一个参数是cron表达式（分 时 日 月 周），第二个参数是具体的任务执行逻辑，和`phalapi-cli`的表示习惯一致。

`outPutLog` 自定义日志输出的位置，不使用该方法则不记录日志

### 接入步骤3：

需要确保`phalapi-cron`和`phalapi-cli`两个文件都是可执行文件，需要注意的是，有可能文件的读写属性不能记录到git仓库，导致服务器拉下来以后文件没有可执行属性。需要提交之前进行属性重写

`git update-index --chmod=+x .\bin\cron`

`git update-index --chmod=+x .\bin\cli`

部署以后在服务器crontab中添加一条分钟定时任务就会每分钟检测`Schedule`中是否有需要执行的任务，如果有就会自动执行：

```
# 每分钟执行定时任务
* * * * * php /var/www/html/bin/phalapi-cron 
```



