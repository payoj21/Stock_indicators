
import com.mysql.jdbc.PreparedStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import com.tictactec.ta.lib.*;
import java.util.Scanner;
public class Pattern {
    private Connection con;
    private Statement st;
    private ResultSet rs;
    // DBConnect to make connection with the mysql localhost
    public Pattern(){
        try{
            long timeNow3 = System.currentTimeMillis();
                System.out.println(timeNow3);
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/companies","root","root");
            st = con.createStatement();
            long timeNow = System.currentTimeMillis();
                System.out.println(timeNow);
        }
        catch(Exception e){
            System.out.println("Error "+e);
        }
    }
    double minimum(double closeprice,double openprice){
        
        if(closeprice<openprice)
            return closeprice;
        else
            return openprice;
    }
    double maximum(double closeprice,double openprice){
        
        if(closeprice>openprice)
            return closeprice;
        else
            return openprice;
    }
    
    void uptrend(double[] openprice,double[] closeprice,String date[]){
        
        double max[] = new double[closeprice.length];
        String maxdate[] = new String[date.length];
        max[0] = maximum(closeprice[0],openprice[0]);
        maxdate[0] = date[0];
        
        double price[] = new double[closeprice.length];
        for(int i=0;i<price.length;i++){
            price[i] = maximum(closeprice[i],openprice[i]);
        }
        int x =1;
        for(int i=1;i<price.length-1;i++){
            if((price[i]>price[i+1])&(price[i]>price[i-1])){
                max[x]=price[i];
                maxdate[x]=date[i];
                
            }
            if(max[x]!=0.0)
            System.out.println(" "+max[x]+" "+maxdate[x]);
            x++;
        }
        System.out.println();
        System.out.println();
        double min[] = new double[closeprice.length];
        String mindate[] = new String[date.length];
        min[0] = minimum(closeprice[0],openprice[0]);
        mindate[0] = date[0];
        int y =1;
        double mprice[] = new double[closeprice.length];
        for(int i=0;i<mprice.length;i++){
            mprice[i] = minimum(closeprice[i],openprice[i]);
        }
        for(int i=1;i<mprice.length-1;i++){
            if((mprice[i]<mprice[i+1])&(mprice[i]<mprice[i-1])){
                min[y]=mprice[i];
                mindate[y]=date[i];
            }   
            if(min[y]!=0.0)
            System.out.println(" "+min[y]+" "+mindate[y]);
            y++;
            
        }
        System.out.println();
        System.out.println();
        System.out.println("output");
		// Need changes here. Not correct.
        for(int i =1;i<maxdate.length-1;i++){
            if(max[i]==0.0){
                i++;
            }
            else{
                int j=i+1;
                int ctr = j;
                
                while(max[ctr]==0.0){
                    ctr++;
                }
                
                int a = j;
                int c = j;
                int count = 0;
                while(a<=ctr){
                    if(min[a]!=0.0&&count >= 1){
                        count++;
                    }
                    if(min[a]!=0 && count==0){
                        System.out.println("Downtrend from "+maxdate[i]+" till "+mindate[a]);
                        c=a;
                        count++;
                    }
                    a++;
                }
                if(count==1){
                    System.out.println("Uptrend from "+mindate[c]+" to "+maxdate[ctr]);
                }
                i=a+1;
            }
        }
        
    }
    
    public void Data(){
        try{
            int t;
                System.out.println("Enter Time period");
                Scanner in = new Scanner(System.in);
                t = in.nextInt();
                int [] id = new int[t];
                int [] ListingId = new int[t];
                String [] PriceDate = new String[t];
                double [] TradedQuant = new double[t];
                double [] OpenPrice = new double[t];
                double [] AdjHighPrice = new double[t];
                double [] AdjLowPrice = new double[t];
                double [] AdjClosePrice = new double[t];
        	String sq = "select id,listingid,pricedate,tradedquantity,openprice,adjustedhighprice,adjustedcloseprice,adjustedlowprice from nifty where listingid = 25784 order by pricedate Desc LIMIT "+t;
                
                PreparedStatement s = (PreparedStatement) con.prepareStatement(sq);
                rs = s.executeQuery();
                
                int i =0;
                //Reading various t from database table to get input into different arrays
                while(rs.next()){
                    id[t-i-1] = rs.getInt("ID");
                    ListingId[t-i-1] = rs.getInt("ListingId");
                    PriceDate[t-i-1] = rs.getString("Pricedate");
                    TradedQuant[t-i-1] = rs.getDouble("TradedQuantity");
                    OpenPrice[t-i-1] = rs.getDouble("OpenPrice");
                    AdjHighPrice[t-i-1] = rs.getDouble("AdjustedHighPrice");
                    AdjLowPrice[t-i-1] = rs.getDouble("AdjustedLowPrice");
                    AdjClosePrice[t-i-1] = rs.getDouble("AdjustedClosePrice");
                    i++ ;
                }
               
                uptrend(OpenPrice,AdjClosePrice,PriceDate);
                
         }
         catch(Exception e){
            System.out.println(e);
         }
         
    }
}
