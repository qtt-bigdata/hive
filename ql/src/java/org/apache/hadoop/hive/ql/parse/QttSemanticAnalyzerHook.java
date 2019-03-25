package org.apache.hadoop.hive.ql.parse;

import java.io.Serializable;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.shims.ShimLoader;

public class QttSemanticAnalyzerHook extends AbstractSemanticAnalyzerHook {
    private final static String NO_PARTITION_WARNING = "WARNING: HQL is not efficient, Please specify partition condition! HQL:%s ;USERNAME:%s";
    private final static String SHOW_PARTITIONS_WARNING = "WARNING: HQL is not efficient, Please specify show partitions condition! HQL:%s ;USERNAME:%s";
    private SessionState ss = SessionState.get();
    private final LogHelper console = SessionState.getConsole();
    private Hive hive = null;
    private String username;
    private String currentDatabase = "default";
    private String hql;
    private String whereHql;
    private String tableAlias;
    private String tableName;
    private String tableDatabaseName;
    private String bigTableFilter;
    private Boolean needCheckPartition = false;
    private int allowDayCount = 30;
    Table t;
    boolean isParitioned;

    @Override
    public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast)
            throws SemanticException{

//        if(ast.getToken().getType() == HiveParser.TOK_QUERY){
//
//            throw new SemanticException("不能执行查询操作");
//        }


//        return ast;

        if(ss == null){
            try {
                ss = new SessionState(context.getHive().getConf());
            } catch (HiveException e) {
                e.printStackTrace();
            }
            ss.setIsHiveServerQuery(true); // All is served from HS2, we do not need e.g. Tez sessions
            SessionState.start(ss);
            console.printInfo(ss.toString());
        }

        if(ss.getLastCommand() == null){
            //console.printInfo("getlastcommand is null");
        }

        hql = context.getCommand().trim().toLowerCase();
        hql = StringUtils.replaceChars(hql, '\n', ' ');
        if (hql.contains("where")) {
            whereHql = hql.substring(hql.indexOf("where"));
        }

        try {
            hive = context.getHive();
            currentDatabase = ss.getCurrentDatabase();
        } catch (HiveException e) {
            throw new SemanticException(e);
        }

        //过滤非法的set操作
        HiveConf conf = hive.getConf();
        //打印点东西看一下
        //console.printInfo(conf.get("hive.qtt.bigtable.filter"));
        bigTableFilter = conf.get("hive.qtt.bigtable.filter");
        console.printInfo(hql);
        username = ss.getUserName();
        if(hql.startsWith("set")){
            console.printInfo(conf.get("hive.qtt.set.filter"));
            //不允许的set操作
            String unSetOperation =  conf.get("hive.qtt.set.filter");
            String[] listSplitSet = hql.trim().split(" ");
            String nowSetKey = listSplitSet[1].split("=")[1];
            console.printInfo(nowSetKey);

            if(unSetOperation.contains(nowSetKey)){
                throw new SemanticException(String.format("hql:%s 被禁止操作"));

            }

        }

        switch (ast.getToken().getType()) {

            //show partitions拦截
            case HiveParser.TOK_SHOWPARTITIONS:
                if(hql.substring(hql.indexOf("partitions") + 10).contains("partition")){
                    break;
                }
                throw new SemanticException("非法的show partitions操作: "+String.format(SHOW_PARTITIONS_WARNING, hql, username));
            case HiveParser.TOK_QUERY:


                extractFromClause((ASTNode) ast.getChild(0));

                if (needCheckPartition && !StringUtils.isBlank(tableName)) {
                    String dbname = StringUtils.isEmpty(tableDatabaseName) ? currentDatabase
                            : tableDatabaseName;
                    String tbname = tableName;
                    String[] parts = tableName.split(".");
                    if (parts.length == 2) {
                        dbname = parts[0];
                        tbname = parts[1];
                    }

                    try {
                        t = hive.getTable(dbname, tbname);
                        isParitioned = t.isPartitioned();
                    }catch (HiveException e){
                        e.printStackTrace();
                    }
                    if (isParitioned) {
                        if (StringUtils.isBlank(whereHql)) {
                            throw new SemanticException("输入的partition不能为空: "+String.format(NO_PARTITION_WARNING, hql, username));
                        } else {
                            List<FieldSchema> partitionKeys = t.getPartitionKeys();
                            List<String> partitionNames = new ArrayList<String>();
                            for (int i = 0; i < partitionKeys.size(); i++) {
                                partitionNames.add(partitionKeys.get(i).getName().toLowerCase());
                            }

                            if (!containsPartCond(partitionNames, whereHql, tableAlias)) {
                                throw new SemanticException("查询操作请指定partition: "+String.format(NO_PARTITION_WARNING, hql, username));
                            }
                        }
                    }
                }
                break;
            case HiveParser.TOK_CREATEDATABASE:
            case HiveParser.TOK_CREATETABLE:
            case HiveParser.TOK_ALTERTABLE:
//                if(hql.contains("location")){
//                    if(!hql.contains("/user/hive/warehouse/"))
//                        throw new SemanticException("the data path must be under /user/hive/warehouse/");
//                }
                break;
            default:
                break;

        }

        return ast;
    }

    private boolean containsPartCond(List<String> partitionKeys, String sql, String alias) {
        for (String pk : partitionKeys) {
            if (sql.contains(pk)) {
                return true;
            }
            if (!StringUtils.isEmpty(alias) && sql.contains(alias + "." + pk)) {
                return true;
            }
        }
        return false;
    }

    //查询过程是否partititon操作过滤
    private void extractFromClause(ASTNode ast) {
        if (HiveParser.TOK_FROM == ast.getToken().getType()) {
            ASTNode refNode = (ASTNode) ast.getChild(0);
            if (refNode.getToken().getType() == HiveParser.TOK_TABREF && ast.getChildCount() == 1) {
                ASTNode tabNameNode = (ASTNode) (refNode.getChild(0));
                int refNodeChildCount = refNode.getChildCount();
                if (tabNameNode.getToken().getType() == HiveParser.TOK_TABNAME) {
                    if (tabNameNode.getChildCount() == 2) {
                        tableDatabaseName = tabNameNode.getChild(0).getText().toLowerCase();
                        tableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabNameNode.getChild(1))
                                .toLowerCase();
                    } else if (tabNameNode.getChildCount() == 1) {
                        tableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabNameNode.getChild(0))
                                .toLowerCase();
                    } else {
                        return;
                    }

                    if (refNodeChildCount == 2) {
                        tableAlias = BaseSemanticAnalyzer.unescapeIdentifier(refNode.getChild(1).getText())
                                .toLowerCase();
                    }
                    needCheckPartition = true;
                }
            }
        }
    }

    @Override
    public void postAnalyze(HiveSemanticAnalyzerHookContext context,
                            List<Task<? extends Serializable>> rootTasks) throws SemanticException {
        HiveConf conf = hive.getConf();
        String[] dayKeyList = conf.get("hive.partition.day.keys").trim().split(",");
        allowDayCount = Integer.parseInt(conf.get("hive.normal.table.day.limit").trim());
        Set<ReadEntity> readEntitys = context.getInputs();
        int count = 0;
        //过滤大表超过一天的查询操作
        Map<String, Set<String>> bigTableToFilter = new HashMap<>();

        //正常表的超过30天的查询操作
        Map<String, Set<String>> normalTableToFilter = new HashMap<>();

        console.printInfo("Total Read Entity Size:" + readEntitys.size());


        for (ReadEntity readEntity : readEntitys) {
            Partition p = readEntity.getPartition();
            if(p != null){
                Table t = readEntity.getTable();
                //console.printInfo("Partition information: " + p + "\n");
                //console.printInfo("Table information: "+t+"\n");
                String partitionLine = p.toString();
                String tableName = t.toString().trim();


                //判断是不是大表的partition
                if(bigTableFilter.contains(tableName)){
                    for(String dayKey : dayKeyList){
                        if(partitionLine.contains(dayKey)){
                            String dayValue = parseDayValue(dayKey, partitionLine);
                            if(!bigTableToFilter.containsKey(tableName)){
                                bigTableToFilter.put(tableName, new HashSet<String>());
                            }
                            bigTableToFilter.get(tableName).add(dayValue);
                            if(bigTableToFilter.get(tableName).size() > 1){
                                throw new SemanticException("大表查询不能超过一天");
                            }

                        }
                    }

                }else{
                    for(String dayKey : dayKeyList){
                        if(partitionLine.contains(dayKey)){
                            String dayValue = parseDayValue(dayKey, partitionLine);
                            if(!normalTableToFilter.containsKey(tableName)){
                                normalTableToFilter.put(tableName, new HashSet<String>());
                            }
                            normalTableToFilter.get(tableName).add(dayValue);
                            if(normalTableToFilter.get(tableName).size() > allowDayCount){
                                throw new SemanticException("普通表查询不能超过"+allowDayCount+"天");
                            }

                        }
                    }

                }
            }

        }
    }

    //解析每一个partition是否是一天的量
    private String parseDayValue(String dayKey, String partitionLine){
        if(partitionLine.contains("/")){
            String[] keyList = partitionLine.split("/");
            for(String key : keyList){
                if(key.contains(dayKey)){
                    //规范化key为data=20190311这种
                    String formatKey = key.substring(key.indexOf(dayKey));
                    return formatKey.split("=")[1];
                }
            }

        }

        return partitionLine.substring(partitionLine.indexOf(dayKey)).split("=")[1];

    }
}

