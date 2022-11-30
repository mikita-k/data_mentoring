# Databricks notebook source
# MAGIC %md
# MAGIC Markdown
# MAGIC 
# MAGIC # Title
# MAGIC ## Title
# MAGIC ### Title
# MAGIC 
# MAGIC Ordered list:
# MAGIC 
# MAGIC 0. el 1
# MAGIC 0. el 2
# MAGIC 0. el 3
# MAGIC 
# MAGIC Non ordered list
# MAGIC * asd
# MAGIC * qwe
# MAGIC 
# MAGIC | name       | value |
# MAGIC |------------|-------|
# MAGIC | Potato     | 2 kg  |
# MAGIC | Persimonai | 1 vnt |

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "I'm a SQL"

# COMMAND ----------

login = "user"
password = "********"
user_dict={
    "Login": login,
    "Password": password
}

# COMMAND ----------

print(f'Login:    {user_dict["Login"]}')
print(f'Password: {user_dict["Password"]}')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${login}' as login_var,
# MAGIC        '${user_dict["Login"]}' as login_from_dict

# COMMAND ----------

display(dbutils.fs.ls("/"))
