package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import db.DB;
import db.DbException;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

public class SellerDaoJDBC implements SellerDao{
	
	private final static String SQL_SELECT = "SELECT a.id, a.name, a.email, a.birthdate, a.basesalary, a.departmentId, b.name as depName " 
			+ "FROM seller a "
			+ "INNER JOIN department b "
			+ "ON a.departmentId = b.id "
			+ "WHERE a.id = ?";
	
	private Connection conn; 
	
	public SellerDaoJDBC(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void insert(Seller seller) {
		// TODO Auto-generated method stub		
	}

	@Override
	public void update(Seller seller) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteById(Integer id) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Seller findById(Integer id) {
		
		PreparedStatement st = null;
		ResultSet rs = null;
		
		try {
			
			st = conn.prepareStatement(SQL_SELECT);
			st.setInt(1, id);
			rs = st.executeQuery();
			
			if(rs.next()) {
				Department dep = new Department();
				dep.setId(rs.getInt("departmentId"));
				dep.setName(rs.getString("DepName"));
				
				Seller obj = new Seller();
				obj.setId(rs.getInt("id"));
				obj.setName(rs.getString("name"));
				obj.setEmail(rs.getString("email"));
				obj.setBaseSalary(rs.getDouble("baseSalary"));
				obj.setBirthDate(rs.getDate("birthDate"));
				obj.setDepartment(dep);
				return obj;				
			}
			return null;
			
		} catch(SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
		
	}

	@Override
	public List<Seller> findAll() {
		// TODO Auto-generated method stub
		return null;
	}

}
