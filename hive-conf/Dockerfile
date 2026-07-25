FROM apache/hive:3.1.3

USER root

# انسخ ملف الإعدادات
COPY hive-site.xml /opt/hive/conf/hive-site.xml

# تأكد من الصلاحيات
RUN chmod 644 /opt/hive/conf/hive-site.xml

USER hive