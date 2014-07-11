import com.mysql.jdbc.PreparedStatement;
import java.sql.*;
import java.util.Iterator;
import java.util.*;
import com.tictactec.ta.lib.*;
import java.text.SimpleDateFormat;

public class DBConnect {
    private Connection con;
    private Statement st;
    private ResultSet rs;
    // DBConnect to make connection with the mysql localhost
    public DBConnect(){
        try{
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/companies","root","root");
            st = con.createStatement();
        }
        catch(Exception e){
            System.out.println("Error "+e);
        }      
    }
    // All calculations are done in the function Data()
    // 1) Taking data from the database into the virtual memory
    // 2) Doing calculations
    // 3) Inserting the output into the database table
    public void Data(){
        // Time Period for various indicators
        int bband = 20; // Bollinger Band Time Period
        int ccitp = 20; // CCI Time Period
        int rsitp =14;  // RSI Time Period
        int ematpf = 20; // Fast EMA Time Period 
        int ematps = 50; // Slow EMA Time Period
        int smatpf = 20; // Fast SMA Time Period
        int smatps = 50; // Slow SMA Time Period
        int macdtpf = 12; // MACD Fast Time Period
        int macdtps = 26; // MACD Slow Time Period
        int macdsig = 9; // MACD Signal Time Period
        int atrtp = 14; // Average True Range Time Period
        int adxtp = 14; // Average Directional Index Time Period
        int stochf = 14; // Stochastic Fast
        int stochs = 3; // Stochastic Slow
        int stochd = 3; // Stochastic D
        int mfitp = 14; // Money Flow Index Time Period
        int aroontp = 14; // Aroon Oscillator time period
        int willr = 14; //William %R time period
        int roctp=14; //ROC Time Period
        int ultosc1=7; //Ultimate Oscillator Time Period 
        int ultosc2=14; //Ultimate Oscillator Time Period 
        int ultosc3=28; //Ultimate Oscillator Time Period 
        try{ 
            // Getiing the largest date available in the table nifty_studies
            rs = st.executeQuery("Select MAX(PriceDate) from nifty_studies;"); 		
            rs.next();
            Timestamp datetime = rs.getTimestamp("MAX(PriceDate)"); 
            System.out.println(datetime);
             
             // If number of rows in nifty_studies = 0 then calculate the values from the starting else UPDATE. 
             if(datetime == null){
               
                // Query to count total number of rows in table nifty
               	rs = st.executeQuery("Select Count(ListingID) from nifty;");
		rs.next();
		int rows = rs.getInt(1);
                //rows is total number of rows present in the table nifty
		//arrays to get data for corresponding required columns in table nifty
		int [] id = new int[rows];
                int [] ListingId = new int[rows];
                String [] PriceDate = new String[rows];
                double [] TradedQuant = new double[rows];
                double [] AdjHighPrice = new double[rows];
                double [] AdjLowPrice = new double[rows];
                double [] AdjClosePrice = new double[rows];
		
                long timeNow = System.currentTimeMillis();
                System.out.println(timeNow);
                
                // Query to select required columns in table nifty into virtual memory
                String sq = "select id,listingid,pricedate,tradedquantity,adjustedhighprice,adjustedcloseprice,adjustedlowprice from nifty order by listingid,pricedate ;";
                
                PreparedStatement s = (PreparedStatement) con.prepareStatement(sq);
                rs = s.executeQuery();
                
                int i =0;
                //Reading various rows from database table to get input into different arrays
                while(rs.next()){
                    id[i] = rs.getInt("ID");
                    ListingId[i] = rs.getInt("ListingId");
                    PriceDate[i] = rs.getString("Pricedate");
                    TradedQuant[i] = rs.getDouble("TradedQuantity");
                    AdjHighPrice[i] = rs.getDouble("AdjustedHighPrice");
                    AdjLowPrice[i] = rs.getDouble("AdjustedLowPrice");
                    AdjClosePrice[i] = rs.getDouble("AdjustedClosePrice");
                    i++;
                }
        int x = ListingId.length;
    
    
       int[] HASH=new int[1000000];
        
        int b = 0;
        for(int y = 0; y < x; y++)
        {
            if(HASH[ListingId[y]]==0)
            {  
               HASH[ListingId[y]]=1;
               b++;
            }
        }
        for(int y =0;y<1000000;y++){
            HASH[y]=0;
        }
        int [] listingid = new int[b];
        int d=0;
        for(int y=0;y<x;y++){
            if(HASH[ListingId[y]]==0)
            {  
              HASH[ListingId[y]]=1;
              listingid[d]=ListingId[y];
              d++;
            }
        }
    System.out.println(b);
    
                long timeNow1 = System.currentTimeMillis();
                System.out.println(timeNow1);
                 int l=0;
                  int[] fid = new int[rows];
                  int[] flistingid = new int[rows];
                  double[] fadjustedclose = new double[rows];
                  double[] fadjustedhigh = new double[rows];
                  double[] fadjustedlow = new double[rows];
                  String[] fpricedate = new String[rows];
                  double[] ftradedQuant = new double[rows];
                  double[] frsi = new double[rows];
                  double[] fupperbband = new double[rows];
                  double[] flowerbband = new double[rows];
                  double[] fmidbband = new double[rows];
                  double[] fcci = new double[rows];
                  double[] fsma20 = new double[rows];
                  double[] fsma50 = new double[rows];
                  double[] fema20 = new double[rows];
                  double[] fema50 = new double[rows];
                  double[] fmacdline = new double[rows];
                  double[] fmacdsignal = new double[rows];
                  double[] fhistogram = new double[rows];
                  double[] fadx = new double[rows];
                  double[] fatr = new double[rows];
                  double[] fmfi = new double[rows];
                  double[] fstochk = new double[rows];
                  double[] fstochd = new double[rows];
                  double[] froc = new double[rows];
                  double[] fwillr = new double[rows];
                  double[] faroon = new double[rows];
                  double[] fultosc = new double[rows];
                 
                // Calculating different indicator values stockwise.
                // the for loop is executed on each listingid
		for(int z=0;z<b;z++){
                    int idrows = 0;
                     //idrows is the total number of rows for a particular stock
                    for(int a=0;a<rows;a++){
                        if(ListingId[a]==listingid[z])
                            idrows++;
                    }
                    System.out.println(idrows);
                        //arrays to get data fro a particular stock.
                        int [] tID = new int[idrows];
			int [] tListingId = new int[idrows]; 
			String [] tPriceDate = new String[idrows];
			double [] tTradedQuant = new double[idrows];
			double [] tAdjHighPrice = new double[idrows];
			double [] tAdjLowPrice = new double[idrows];
			double [] tAdjClosePrice = new double[idrows];
                        
                        int y=0;
                        //getting data into arrays for each listingid 
			for(int w=0;w<rows;w++){							
                            if((ListingId[w]==listingid[z])&&(y<idrows)){
                                tID[y] = id[w];
                                tListingId [y] = ListingId[w]; 
                                tPriceDate[y] = PriceDate[w];
                                tTradedQuant[y] = TradedQuant[w];
                                tAdjHighPrice[y] = AdjHighPrice[w];
                                tAdjClosePrice[y] = AdjClosePrice[w];
                                tAdjLowPrice[y] = AdjLowPrice[w];	
                                y++;
                            }
                        }
			
                        // Calculating values of various Indicators
                        
                        Core c = new Core(); //object of ta_lib
                        
                        //Bollinger Bands
                        double outmid [] = new double[idrows]; //array for middle bollinger band
                        double outupper [] = new double[idrows];//array for upper bollinger band
                        double outlower [] = new double[idrows];//array for lower bollinger band
                        double upperstdmult = 2.0; //multiplying factor for upper band
                        double lowerstdmult = 2.0; //multiplying factor for lower band
                        
                        if(bband <idrows){ //bollinger bands are calculated if number of rows is greater than time period for bollinger band else dropped
                            MAType m1 = MAType.Sma; //Simple Moving Average is used. If Exponential or any other moving average is used then the above if condition will change according to the smoothing requirement.
                            MInteger begin9 = new MInteger(); 
                            MInteger length9 = new MInteger();
                            RetCode retCode = c.bbands(0, tAdjClosePrice.length - 1, tAdjClosePrice, bband,upperstdmult, lowerstdmult,m1,   begin9, length9, outupper,outmid,outlower);
                            //Code to shift the values in the array.
                            for(int w=idrows-bband;w>=0;w--){
                                outlower[w+bband-1] = outlower[w];
                            }
                            for(int w=0;w<bband-1;w++){
                                outlower[w] = 0;
                            }
                            
                            for(int w=idrows-bband;w>=0;w--){
                                outmid[w+bband-1] = outmid[w];
                            }
                            for(int w=0;w<bband-1;w++){
                                outmid[w] = 0;
                            }
                            
                            for(int w=idrows-bband;w>=0;w--){
                                outupper[w+bband-1] = outupper[w];
                            }
                            for(int w=0;w<bband-1;w++){
                                outupper[w] = 0;
                            }
                        
                        
                        }
                        
                        //CCI
                        double outcci [] = new double[idrows]; //array for cci
                        if(idrows > ccitp){ //cci is calculated if the number of rows for the listingid is greater than the cci time period
                            MInteger begin10 = new MInteger();
                            MInteger length10 = new MInteger();
                        
                        RetCode retCode2 = c.cci(0,tAdjClosePrice.length-1,tAdjHighPrice,tAdjLowPrice,tAdjClosePrice, ccitp, begin10, length10, outcci);
                        //Code to shift the values in the array.
                        for(int w=idrows-ccitp;w>=0;w--){
                            outcci[w+ccitp-1] = outcci[w];
                        }
                        for(int w=0;w<ccitp-1;w++){
                            outcci[w] = 0;
                        }
                       
                        }
                        
                        //RSI
                        double outrsi[] = new double[idrows];//array for RSI
                        if(idrows > 200){ //as RSI uses smoothing, minimum number of rows required for a particular listingid is 200.
                        MInteger begin = new MInteger();
                        MInteger length = new MInteger();                      
                        
                        RetCode retCode = c.rsi(0, tAdjClosePrice.length - 1, tAdjClosePrice, rsitp,   begin, length, outrsi);
                        // Code to shift values in the array . 
                        for(int w=idrows-rsitp-1;w>=0;w--){
                            outrsi[w+rsitp] = outrsi[w];
                        }
                        for(int w=0;w<rsitp;w++){
                            outrsi[w] = 0;
                        }
                        }
                        
                        
                        //ATR
                        double outatr[] = new double[idrows];
                        if(idrows > 200){ //as ATR uses smoothing, minimum number of rows for a particular listingid is 200
                        MInteger begin6 = new MInteger();
                        MInteger length6 = new MInteger();
                        
                        RetCode retCode6 = c.atr(0,tAdjClosePrice.length-1, tAdjHighPrice,tAdjLowPrice,tAdjClosePrice, atrtp, begin6, length6, outatr);
                        // Code to shift values in the array . 
                        for(int w=idrows-1;w>=atrtp;w--){
                            outatr[w] = outatr[w-atrtp];
                        }
                        for(int w=0;w<atrtp-1;w++){
                            outatr[w] = 0;
                        }
                        }
                        
                        
                        //ADX
                        double outadx[] = new double[idrows];
                        if(idrows > 200){ //as ATR uses smoothing, minimum number of rows for a particular listingid is 200
                        MInteger begin3 = new MInteger();
                        MInteger length3 = new MInteger();
                        
                        RetCode retCode3 = c.adx(0,tAdjClosePrice.length-1, tAdjHighPrice,tAdjLowPrice,tAdjClosePrice, adxtp, begin3, length3, outadx);
                        // Code to shift values in the array . 
                        for(int w=idrows-1;w>=2*adxtp-1;w--)
                            outadx[w] = outadx[w-2*adxtp+1];
                        for(int w=0;w<(2*adxtp)-1;w++){
                            outadx[w] = 0;
                        }
                        }
                        
                        
                        //SMA
                        double outsma20[] = new double[idrows]; // array to calculate SMA for timeperiod 20
                        double outsma50[] = new double[idrows];// array to calculate SMA for timeperiod 20
                        //Code to calculate SMA20
                        if(smatpf <= idrows){ //SMA is calculated if the number of rows for the listingid is greater than the SMA time period
                        MInteger begin2 = new MInteger();
                        MInteger length2 = new MInteger();
                        
                        RetCode retCode2 = c.sma(0,tAdjClosePrice.length-1, tAdjClosePrice, smatpf, begin2, length2, outsma20);
                        //Below code is to shift the values in the array
                        for(int w=idrows-smatpf;w>=0;w--){
                            outsma20[w+smatpf-1] = outsma20[w];
                        }
                        for(int w=0;w<smatpf-1;w++){
                            outsma20[w] = 0;
                        }
                        }
                        //Code to calculate SMA50
                        if(smatps <= idrows){//SMA is calculated if the number of rows for the listingid is greater than the SMA time period
                        MInteger begin5 = new MInteger();
                        MInteger length5 = new MInteger();
                        RetCode retCode5 = c.sma(0,tAdjClosePrice.length-1, tAdjClosePrice, smatps, begin5, length5, outsma50);
                        for(int w=idrows-smatps;w>=0;w--){
                            outsma50[w+smatps-1] = outsma50[w];
                        }
                        for(int w=0;w<smatps-1;w++){
                            outsma50[w] = 0;
                        }
                        }
                        
                        
                        //EMA
                        double outema20[] = new double[idrows];
                        double outema50[] = new double[idrows];
                        if(idrows > 200){
                        //Code to calculate EMA20
                        MInteger begin0 = new MInteger();
                        MInteger length0 = new MInteger();
                        
                        RetCode retCode0 = c.ema(0,tAdjClosePrice.length-1, tAdjClosePrice, ematpf, begin0, length0, outema20);
                        //Below code is to shift the values in the array
                        for(int w=idrows-ematpf;w>=0;w--){
                            outema20[w+ematpf-1] = outema20[w];
                        }
                        for(int w=0;w<ematpf-1;w++){
                            outema20[w] = 0;
                        }
                
                        //Code to calculate EMA50
                        MInteger begin4 = new MInteger();
                        MInteger length4 = new MInteger();
                        RetCode retCode4 = c.ema(0,tAdjClosePrice.length-1, tAdjClosePrice, ematps, begin4, length4, outema50);
                        //Below code is to shift the values in the array
                        for(int w=idrows-ematps;w>=0;w--){
                            outema50[w+ematps-1] = outema50[w];
                        }
                        for(int w=0;w<ematps-1;w++){
                            outema50[w] = 0;
                        }
                        }
                        
                        
                        //MACD
                        double outmacd[] = new double[idrows]; // array for macdline
                        double outsignal[] = new double[idrows];// array for signal 
                        double outhistogram[] = new double[idrows];// array for histogram
                        
                        if(idrows > 200){ 
                        MInteger begin1 = new MInteger();
                        MInteger length1 = new MInteger();
                        
                        RetCode retCode1 = c.macd(0, tAdjClosePrice.length - 1, tAdjClosePrice, macdtpf,macdtps,macdsig,begin1, length1, outmacd,outsignal,outhistogram);
                        //Below code is to shift the values in the array
                        for(int w=idrows-macdtps-macdsig+1;w>=0;w--){
                            outmacd[w+macdtps+macdsig-2] = outmacd[w];
                        }
                        for(int w=0;w<macdtps+macdsig-2;w++){
                            outmacd[w] = 0;
                        }
                        for(int w = idrows-macdsig-macdtps+1;w>=0;w--){
                            outsignal[w+macdsig+macdtps-2] = outsignal[w];
                        }
                        for(int w= 0;w<macdsig+macdtps-2;w++)
                           outsignal[w] = 0;
                        for(int w = idrows-macdsig-macdtps+1;w>=0;w--){
                            outhistogram[w+macdsig+macdtps-2] = outhistogram[w];
                        }
                        for(int w= 0;w<macdsig+macdtps-2;w++)
                            outhistogram[w] = 0;
                        }    
                        
                        
                        //MFI
                        //This indicator uses Volume so this indicator cannot be apply on indices.
                        double outmfi[] = new double [idrows]; // array for MFI
                        int ctr =0;
                        for(int k=0;k<tTradedQuant.length;k++){ // check on volume for checking whether the lisitngid is an index or not
                            if(tTradedQuant[k]==0)
                                ctr++;
                        }
                        if(ctr!=tTradedQuant.length){ // if count is equal to the length of array tTradedQuant then the listingid is an index else not 
                        if(mfitp < idrows ){
                        MInteger begin7 = new MInteger();
                        MInteger length7 = new MInteger();
                        
                        RetCode retCode7 = c.mfi(0, tAdjClosePrice.length - 1, tAdjHighPrice,tAdjLowPrice,tAdjClosePrice,tTradedQuant, mfitp,begin7, length7,outmfi);
                        
                        for(int w=idrows-mfitp-1;w>=0;w--){
                            outmfi[w+mfitp] = outmfi[w];
                        }
                        for(int w = 0;w<mfitp;w++)
                            outmfi[w]=0;
                        }
                        }
        
                // A/D Line
              /*  
                double outad[] = new double [idrows];
                MInteger begin11 = new MInteger();
                MInteger length11 = new MInteger();
                RetCode retCode11 = c.ad(0, tAdjClosePrice.length - 1,tAdjHighPrice,tAdjLowPrice,tAdjClosePrice,tTradedQuant,begin11, length11,outad);
                
                */
                    
                //Aroon Oscillator 
                
                double outaroon[] = new double [idrows];
                if(idrows>aroontp){
                MInteger begin12 = new MInteger();
                MInteger length12 = new MInteger();
                RetCode retCode12 = c.aroonOsc(0, tAdjClosePrice.length - 1,tAdjHighPrice,tAdjLowPrice,aroontp,begin12, length12,outaroon);
                
                for(int w=idrows-aroontp-1;w>=0;w--){
                            outaroon[w+aroontp] = outaroon[w];
                }
                for(int w = 0;w<aroontp;w++)
                    outaroon[w]=0;
                }   
                //William %R
                double outwill[] = new double [idrows];
                if(idrows>=willr){
                    MInteger begin8 = new MInteger();
                MInteger length8 = new MInteger();
                RetCode retCode = c.willR(0,tAdjClosePrice.length-1,tAdjHighPrice,tAdjLowPrice,tAdjClosePrice,willr,begin8,length8,outwill);
                
                for(int w=outwill.length-willr;w>=0;w--){
                    outwill[w+willr-1]=outwill[w];
                }
                }
                //Stochastic RSI
                
                //ROC
                double outroc[] = new double[idrows];  //12
                if(idrows>=roctp){
                        MInteger begin8 = new MInteger();
                MInteger length8 = new MInteger();
                RetCode retCode = c.roc(0,tAdjClosePrice.length-1,tAdjClosePrice,roctp,begin8,length8,outroc);
                
                for(int w=outroc.length-1;w>=roctp;w--){
                    outroc[w]=outroc[w-roctp];
                }
                }
                
                //ULTIMATE OSCILLATOR
                double outultosc[] = new double[idrows]; //29
                if(idrows>=ultosc3){
                MInteger begin8 = new MInteger();
                MInteger length8 = new MInteger();
                RetCode retCode = c.ultOsc(0,tAdjClosePrice.length-1,tAdjHighPrice,tAdjLowPrice,tAdjClosePrice,ultosc1,ultosc2,ultosc3,begin8,length8,outultosc);
                
                for(int w=outultosc.length-1;w>=ultosc3;w--){
                    outultosc[w]=outultosc[w-ultosc3];
                }
                }
                //Stochastic
                double outstochk[] = new double[idrows];
                double outstochd[] = new double[idrows];
                if(stochf+stochs <idrows){
                MInteger begin8 = new MInteger();
                MInteger length8 = new MInteger();
                MAType m = MAType.Sma;
             
                RetCode retCode8 = c.stoch(0,tAdjClosePrice.length-1, tAdjHighPrice,tAdjLowPrice,tAdjClosePrice, stochf,stochs,m,stochd,m,begin8, length8, outstochk,outstochd);
                
               
                for(int w=idrows-stochf-stochs-1;w>=0;w--){
                    outstochk[w+stochf+stochs] = outstochk[w];
                }
                for(int w=0;w<stochf+stochs-1;w++){
                    outstochk[w] = 0;
                }
                
                
                for(int w=idrows-stochf-stochd-1;w>=0;w--){
                    outstochd[w+stochd+stochf] = outstochd[w];
                }
                for(int w=0;w<stochd+stochf-1;w++){
                    outstochd[w] = 0;
                }
           
           
                }
                
                for(int w=0;w<idrows;w++){
                  fid[l] = tID[w];
                  flistingid[l] = tListingId[w];
                  fadjustedclose[l] = tAdjClosePrice[w];
                  fadjustedhigh[l] = tAdjHighPrice[w];
                  fadjustedlow[l] = tAdjLowPrice[w];
                  fpricedate[l] = tPriceDate[w];
                  ftradedQuant[l] = tTradedQuant[w];
                  frsi[l] = outrsi[w];
                  fupperbband[l] = outupper[w];
                  flowerbband[l] = outlower[w];
                  fmidbband[l] = outmid[w];
                  fcci[l] = outcci[w];
                  fsma20[l] = outsma20[w];
                  fsma50[l] = outsma50[w];
                  fema20[l] = outema20[w];
                  fema50[l] = outema50[w];
                  fmacdline[l] = outmacd[w];
                  fmacdsignal[l] = outsignal[w];
                  fhistogram[l] = outhistogram[w];
                  fadx[l] = outadx[w];
                  fatr[l] = outatr[w];
                  fmfi[l] = outmfi[w];
                  fstochk[l] = outstochk[w];
                  fstochd[l] = outstochd[w];
                  froc[l] = outroc[w];
                  fwillr[l] = outwill[w];
                  faroon[l] = outaroon[w] ;
                  fultosc[l] = outultosc[w];
                  l++;
                }
                
            }
                // Code for inserting Indicator values in the table nifty_studies stockwise
                try{
                    con.setAutoCommit(false);
                    System.out.println("Inserting");
                    String sql = "INSERT INTO nifty_studies VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
                    PreparedStatement pst = (PreparedStatement) con.prepareStatement(sql);
                    int batchSize = 100;
                    for(int j=0;j<rows;j++){
                        pst.setDouble(1,fid[j]);
                        pst.setDouble(2,flistingid[j]);
                        pst.setDouble(3,fadjustedclose[j]);
                        pst.setDouble(4,fadjustedhigh[j]);
                        pst.setDouble(5,fadjustedlow[j]);
                        pst.setDouble(6,ftradedQuant[j]);
                        pst.setString(7,fpricedate[j]);
                        pst.setDouble(8,frsi[j]);
                        pst.setDouble(9,fupperbband[j]);
                        pst.setDouble(10,fmidbband[j]);
                        pst.setDouble(11,flowerbband[j]);
                        pst.setDouble(12,fcci[j]);
                        pst.setDouble(13,fstochk[j]);
                        pst.setDouble(14,fstochd[j]);
                        pst.setDouble(15,fsma20[j]);
                        pst.setDouble(16,fsma50[j]);
                        pst.setDouble(17,fema20[j]);
                        pst.setDouble(18,fema50[j]);
                        pst.setDouble(19,fmacdline[j]);
                        pst.setDouble(20,fmacdsignal[j]);
                        pst.setDouble(21,fhistogram[j]); 
                        pst.setDouble(22,fadx[j]);
                        pst.setDouble(23,fatr[j]);
                        pst.setDouble(24,faroon[j]);
                        pst.setDouble(25,fwillr[j]);
                        pst.setDouble(26,froc[j]);
                        pst.setDouble(27,fultosc[j]);
                        pst.setDouble(28,fmfi[j]);
                        pst.addBatch();     
                        if(j%batchSize ==0){
                            System.out.println(j);
                            pst.executeBatch();
                        }
                    
                    }
                    pst.executeBatch();
                    
                    //Once the batch is executed, it is a good pratice to clear the batch
                    //before adding any new query.
                    con.commit();
                    pst.clearBatch();
                    System.out.println("Inserted");
                    }
                    catch(Exception e){
                        System.out.println("Error "+e);
                    }
        }
        else{
            rs = st.executeQuery("Select count(listingid) from nifty where PriceDate >= DATE_SUB('"+datetime+"' , INTERVAL 365 DAY);");	
            rs.next();
            int prows = rs.getInt(1);
            System.out.println(prows);
            int id [] = new int [prows];
            int listingid [] = new int [prows];
            Timestamp PriceDate [] = new Timestamp[prows];
            int TradedQuant [] = new int[prows];
            double AdjHighPrice [] = new double [prows];
            double AdjClosePrice [] = new double [prows];
            double AdjLowPrice [] = new double [prows];
            int i=0;
            rs = st.executeQuery("Select id,listingid,pricedate,tradedquantity,adjustedcloseprice,adjustedhighprice,adjustedlowprice from nifty where PriceDate >= DATE_SUB('"+datetime+"' , INTERVAL 365 DAY) order by listingid,pricedate ;");   
            while(rs.next()){
                id[i] = rs.getInt("ID");
                listingid[i] = rs.getInt("ListingID");
                PriceDate[i] = rs.getTimestamp("PriceDate");
                TradedQuant[i] = rs.getInt("TradedQuantity");
                AdjHighPrice[i] = rs.getDouble("AdjustedHighPrice");
                AdjClosePrice[i] = rs.getDouble("AdjustedClosePrice");
		AdjLowPrice[i] = rs.getDouble("AdjustedLowPrice");
		i++;
            }
            System.out.println(datetime);
             int x = listingid.length;
    
    
       int[] HASH=new int[1000000];
        
        int comp = 0;
        for(int y = 0; y < x; y++)
        {
            if(HASH[listingid[y]]==0)
            {  
               HASH[listingid[y]]=1;
               comp++;
            }
        }
        System.out.println(comp);
    for(int y =0;y<1000000;y++){
            HASH[y]=0;
    }
    //int LID[] = new int[comp];
    int [] LID = new int[comp];
    
    int d=0;
        for(int y=0;y<x;y++){
            if(HASH[listingid[y]]==0)
            {  
              HASH[listingid[y]]=1;
              LID[d]=listingid[y];
              System.out.println(d+" "+LID[d]+" ");
              d++;
            }
        }
        System.out.println();
        for(int a=0;a<comp;a++){
                System.out.println("ListingID: "+LID[a]);
                int idrows = 0;
                int count = 0;
                for(int r=0;r<listingid.length;r++){
                    if(listingid[r]==LID[a])
                        idrows ++;
                    if(listingid[r]==LID[a] && PriceDate[r].after(datetime))
                        count++;
                }
                System.out.println(idrows);
                System.out.println(count);
                if(count>0){
		int [] tID = new int[idrows];
		int [] tListingId = new int[idrows]; 
		Timestamp [] tPriceDate = new Timestamp[idrows];
		double [] tTradedQuant = new double[idrows];
		double [] adjh = new double[idrows];
		double [] adjl = new double[idrows];
		double [] adjc = new double[idrows];
                int y=0;
		for(int b=0;b<listingid.length;b++){                    
                    if(listingid[b]==LID[a]){
                        tID[y] = id[b];
                        tListingId [y] = listingid[b]; 
                        tPriceDate[y] = PriceDate[b];
                        tTradedQuant[y] = TradedQuant[b];
                        adjh[y] = AdjHighPrice[b];
                        adjc[y] = AdjClosePrice[b];
                        adjl[y] = AdjLowPrice[b];	
                        y++;
                    }
		}
		Core c = new Core();
                
                 //Bollinger Bands
                 double outmid [] = new double[bband+count];
                 double outupper [] = new double[bband+count];
                 double outlower [] = new double[bband+count];
                 double upperstdmult = 2.0;
                 double lowerstdmult = 2.0;
                        
                if(bband <count+bband){
                   
                    MAType m1 = MAType.Sma;
                    MInteger begin9 = new MInteger();
                    MInteger length9 = new MInteger();
                    RetCode retCode = c.bbands(adjc.length-bband-count, adjc.length - 1, adjc, bband,upperstdmult, lowerstdmult,m1,   begin9, length9, outupper,outmid,outlower);
                  
                }
                        
                //CCI
                double outcci [] = new double[ccitp+count];
                if(ccitp+count > ccitp){
                    MInteger begin10 = new MInteger();
                    MInteger length10 = new MInteger();

                RetCode retCode2 = c.cci(adjc.length-ccitp-count,adjc.length-1,adjh,adjl,adjc, ccitp, begin10, length10, outcci);
                 
                }
                
		//RSI
		double outrsi[] = new double[idrows];
                if(idrows >= 200){
                    MInteger begin = new MInteger();
                    MInteger length = new MInteger();
                    RetCode retCode = c.rsi(0, adjc.length-1, adjc, rsitp, begin, length, outrsi);
                    for(int w=outrsi.length-rsitp-1;w>=0;w--){
                        outrsi[w+rsitp] = outrsi[w];
       		    }
                    
                }
                //ATR
                double outatr[] = new double[idrows];
                if(idrows >= 200){
                MInteger begin6 = new MInteger();
                MInteger length6 = new MInteger();
                RetCode retCode6 = c.atr(0, adjc.length-1, adjh,adjl,adjc, atrtp, begin6, length6, outatr);
                for(int w=outatr.length-atrtp-1;w>=0;w--){
                    outatr[w+atrtp] = outatr[w];
                }
               
                }
                //ADX
                double outadx[] = new double[idrows];
                if(idrows >= 200){
                MInteger begin3 = new MInteger();
                MInteger length3 = new MInteger();
                RetCode retCode3 = c.adx(0, adjc.length-1, adjh,adjl,adjc, adxtp, begin3, length3, outadx);
                for(int w=outadx.length-1;w>=2*adxtp-1;w--){
                    outadx[w] = outadx[w-2*adxtp+1];
                }
                
                }
                  //Stochastic
                double outstochk[] = new double[stochf+stochs+count];
                double outstochd[] = new double[stochf+stochs+count];
                if(stochf+stochs <stochf+stochs+count){
                MInteger begin8 = new MInteger();
                MInteger length8 = new MInteger();
                MAType m = MAType.Sma;
             
                RetCode retCode8 = c.stoch(adjc.length-count-stochf-stochs,adjc.length-1, adjh,adjl,adjc, stochf,stochs,m,stochd,m,begin8, length8, outstochk,outstochd);
                
                }
              
                
                //SMA
                double outsma20[] = new double[count+smatpf];
                double outsma50[] = new double[count+smatps];
                if(smatpf < count+smatpf){
                MInteger begin2 = new MInteger();
                MInteger length2 = new MInteger();
                RetCode retCode2 = c.sma(adjc.length-smatpf-count,adjc.length-1, adjc, smatpf, begin2, length2, outsma20);
                
                }
                if(smatps < count+smatps){
                MInteger begin4 = new MInteger();
                MInteger length4 = new MInteger();
                RetCode retCode4 = c.sma(adjc.length-smatps-count,adjc.length-1, adjc, smatps, begin4, length4, outsma50);
                
                }
                //EMA
                double outema20[] = new double[idrows];
                double outema50[] = new double[idrows];
                if(idrows >= 200){
                MInteger begin0 = new MInteger();
                MInteger length0 = new MInteger();
                RetCode retCode0 = c.ema(0,adjc.length-1, adjc, ematpf, begin0, length0, outema20);
                for(int w=outema20.length-ematpf;w>=0;w--){
                    outema20[w+ematpf-1] = outema20[w];
                }
                
                MInteger begin5 = new MInteger();
                MInteger length5 = new MInteger();
                RetCode retCode5 = c.ema(0,adjc.length-1, adjc, ematps, begin5, length5, outema50);
                for(int w=outema50.length-ematps;w>=0;w--){
                    outema50[w+ematps-1] = outema50[w];
                }
                
                }
                
                //MACD
                double [] outmacd = new double[idrows];
                double [] outsignal = new double[idrows];
                double [] outhistogram = new double[idrows];
                if(idrows >= 200){
                MInteger begin1 = new MInteger();
                MInteger length1 = new MInteger();
                RetCode retCode1 = c.macd(0, adjc.length-1, adjc, macdtpf, macdtps, macdsig, begin1, length1, outmacd, outsignal, outhistogram);
                for(int w=outmacd.length-macdtps-macdsig+1;w>=0;w--){
                    outmacd[w+macdtps+macdsig-2] = outmacd[w];
                }
                for(int w = outsignal.length-macdsig-macdtps+1;w>=0;w--){
                    outsignal[w+macdsig+macdtps-2] = outsignal[w];
                 }
                for(int w = outhistogram.length-macdsig-macdtps+1;w>=0;w--){
                    outhistogram[w+macdsig+macdtps-2] = outhistogram[w];
                }
                
                }
                //MFI
                double [] outmfi = new double[mfitp+count];
                int ctr = 0;
                for(int k=0;k<tTradedQuant.length;k++){ // check on volume for checking whether the lisitngid is an index or not
                            if(tTradedQuant[k]==0)
                                ctr++;
                }
                if(ctr!=tTradedQuant.length){ // if count is equal to the length of array tTradedQuant then the listingid is an index else not 
                if(mfitp < mfitp+count){
                MInteger begin7 = new MInteger();
                MInteger length7 = new MInteger();
                RetCode reCode7 = c.mfi(adjc.length-mfitp-count,adjc.length-1,adjh,adjl,adjc,tTradedQuant,mfitp,begin7,length7,outmfi);
               
                }
                
                }
                //Aroon Oscillator
                double [] outaroon = new double[aroontp+count];
                if(aroontp+count>aroontp){
                MInteger begin10 = new MInteger();
                MInteger length10 = new MInteger();
                RetCode reCode10 = c.aroonOsc(adjc.length-aroontp-count,adjc.length-1,adjh,adjl,aroontp,begin10,length10,outaroon);
                System.out.println("aroontp");
                }
                //William %R
                double outwill[] = new double [count+willr];
                if(count+willr>willr){
                MInteger begin8 = new MInteger();
                MInteger length8 = new MInteger();
                RetCode retCode = c.willR(adjc.length-willr-count,adjc.length-1,adjh,adjl,adjc,willr,begin8,length8,outwill);
                System.out.println("Willr");
                }
                              
                //ROC
                
                double outroc[] = new double[roctp+count];  
                if(roctp+count>roctp){
                MInteger begin8 = new MInteger();
                MInteger length8 = new MInteger();
                RetCode retCode = c.roc(adjc.length-roctp-count,adjc.length-1,adjc,roctp,begin8,length8,outroc);
                System.out.println("roc");
                }
                
                //ULTOSC
                double outultosc[] = new double[ultosc3+count]; //29
                if(ultosc3+count>ultosc3){
                MInteger begin8 = new MInteger();
                MInteger length8 = new MInteger();
                System.out.println(outultosc.length);
                RetCode retCode = c.ultOsc(adjc.length-ultosc3-count,adjc.length-1,adjh,adjl,adjc,ultosc1,ultosc2,ultosc3,begin8,length8,outultosc);
                for(int w=0;w<outultosc.length;w++){
                    System.out.println(outultosc[w]);
                }
                System.out.println("ultosc");
                }
            try{
                con.setAutoCommit(false);
                
                String sql = "INSERT INTO nifty_studies VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
                PreparedStatement pst = (PreparedStatement) con.prepareStatement(sql);
                int batchSize = 100;
                for(int k=0;k<count;k++){
                    pst.setDouble(1,tID[idrows-count+k]);
                    pst.setDouble(2,tListingId[idrows-count+k]);
                    pst.setDouble(3,adjc[idrows-count+k]);
                    pst.setDouble(4,adjh[idrows-count+k]);
                    pst.setDouble(5,adjl[idrows-count+k]);
                    pst.setDouble(6,tTradedQuant[idrows-count+k]);
                    pst.setTimestamp(7,tPriceDate[idrows-count+k]);
                    pst.setDouble(8,outrsi[idrows-count+k]);
                    pst.setDouble(9,outupper[bband+k]);
                    pst.setDouble(10,outmid[bband+k]);
                    pst.setDouble(11,outlower[bband+k]);
                    pst.setDouble(12,outcci[ccitp+k]);
                    pst.setDouble(13,outstochk[stochf+stochd+k]);
                    pst.setDouble(14,outstochd[stochd+stochf+k]);
                    pst.setDouble(15,outsma20[smatpf+k]);
                    pst.setDouble(16,outsma50[smatps+k]);
                    pst.setDouble(17,outema20[idrows-count+k]);
                    pst.setDouble(18,outema50[idrows-count+k]);
                    pst.setDouble(19,outmacd[idrows-count+k]);
                    pst.setDouble(20,outsignal[idrows-count+k]);
                    pst.setDouble(21,outhistogram[idrows-count+k]);
                    pst.setDouble(22,outadx[idrows-count+k]);
                    pst.setDouble(23,outatr[idrows-count+k]);
                    pst.setDouble(24,outaroon[aroontp+k]);
                    pst.setDouble(25,outwill[willr+k]);
                    pst.setDouble(26,outroc[roctp+k]);
                    pst.setDouble(27,outultosc[ultosc3+k]);
                    pst.setDouble(28,outmfi[mfitp+k]);
                    pst.addBatch();     
                    if(k%batchSize ==0)
                        pst.executeBatch();
          
                }
                        pst.executeBatch();
            
                System.out.println("ListingID: "+LID[a]+"Inserted");
            //Once the batch is executed, it is a good pratice to clear the batch
            //before adding any new query.
                        con.commit();
                        pst.clearBatch();
                    }
                    catch(Exception e){
                        System.out.println("Error "+e);
                    }
                }
            }
            }
        }   
        catch(Exception e){
            System.out.println("error "+e);
        }
        
    }   
}

