package sunny.lab.batch.exec;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

public class WriteToDB {
    public static void main(String[] args) {
        String jdbcURL = "jdbc:mysql://localhost:3306/quizquiz";
        String username = "qzadm";
        String password = "!qzqz";

        String filePath = args[0];
        String fileName = args[1];

        int batchSize = 20;
        int colCnt = 0;

        Connection connection = null;

        try {

            connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setAutoCommit(false);

            StringBuilder sqlBldr = new StringBuilder();
            sqlBldr.append("INSERT INTO ").append(fileName).append(" (");

            if(fileName.equals("qz_question")){
                sqlBldr.append("qid,cid,question,lang,createDate) values (?,?,?,?,?)");
                colCnt = 5;
            }
            if(fileName.equals("qz_answer")){
                sqlBldr.append("aid,qid,cAnswer,wAnswer1,wAnswer2,wAnswer3,lang,createDate) values (?,?,?,?,?,?,?,?)");
                colCnt = 8;
            }
            if(fileName.equals("qz_category")){
                sqlBldr.append("cid,cName,lvl,pCid,lang,createDate) values (?,?,?,?,?,?)");
                colCnt = 6;
            }
            if(fileName.equals("qz_result")){
                sqlBldr.append("rid,uid,cid,silver,gold,createDate,modifyDate) values (?,?,?,?,?,?,?)");
                colCnt = 7;
            }
            if(fileName.equals("qz_status")){
                sqlBldr.append("sid,uid,cid,playCircle,playStage,playTime,createDate,modifyDate) values (?,?,?,?,?,?,?,?)");
                colCnt = 8;
            }
            if(fileName.equals("qz_status")){
                sqlBldr.append("uid,email,passwd,lang,lastLoginDate,createDate,modifyDate) values (?,?,?,?,?,?,?)");
                colCnt = 7;
            }

//            String sql = "INSERT INTO review (course_name, student_name, timestamp, rating, comment) VALUES (?, ?, ?, ?, ?)";
            PreparedStatement statement = connection.prepareStatement(sqlBldr.toString());

            BufferedReader lineReader = new BufferedReader(new FileReader(filePath));
            String lineText = null;

            int count = 0;

            lineReader.readLine(); // skip header line

            while ((lineText = lineReader.readLine()) != null) {
                String[] data = lineText.split(",");
                for(int i=0; i<colCnt; i++){
                    statement.setString(i+1,data[i]);
                }

                statement.addBatch();

                if (count % batchSize == 0) {
                    statement.executeBatch();
                }
            }

            lineReader.close();

            // execute the remaining queries
            statement.executeBatch();

            connection.commit();
            connection.close();

        } catch (IOException ex) {
            System.err.println(ex);
        } catch (SQLException ex) {
            ex.printStackTrace();

            try {
                connection.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

}