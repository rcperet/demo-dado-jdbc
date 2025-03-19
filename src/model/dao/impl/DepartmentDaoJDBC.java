package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import db.DB;
import db.DbException;
import model.dao.DepartmentDao;
import model.entities.Department;

public class DepartmentDaoJDBC implements DepartmentDao {
	
	Connection conn;
	
	private static final String SQL_INSERT = "insert into department (name) values(?)";
	private static final String SQL_SELECT = "SELECT a.id, a.name FROM department a ";
	private static final String SQL_UPDATE = "UPDATE department a "
			+ "set a.name = ? ";
	private static final String SQL_DELETE = "DELETE FROM department a ";
	private static final String WHERE_ID = "WHERE a.id = ?";

	public DepartmentDaoJDBC(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void insert(Department department) {
		
		PreparedStatement st = null;
		
		try {
			
			st = conn.prepareStatement(SQL_INSERT, Statement.RETURN_GENERATED_KEYS);
			st.setString(1, department.getName());
			int rowsAffected = st.executeUpdate();
			
			if(rowsAffected == 0) {
				throw new DbException("Unexpected error! No rows affected");
			}
			
			ResultSet rs = st.getGeneratedKeys();
			if(rs.next()) {
				int id = rs.getInt(1);
				department.setId(id);
			}
			
			DB.closeResultSet(rs);
			
		}catch(SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}

	}

	@Override
	public void update(Department department) {
		PreparedStatement st = null;
		
		try {
			
			st = conn.prepareStatement(SQL_UPDATE + WHERE_ID);
			st.setString(1, department.getName());
			st.setInt(2, department.getId());
			
			st.executeUpdate();
			
		} catch(SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}

	}

	@Override
	public void deleteById(Integer id) {
		PreparedStatement st = null;
		
		try {
			
			st = conn.prepareStatement(SQL_DELETE + WHERE_ID);
			st.setInt(1, id);
			
			st.executeUpdate();
			
		} catch(SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}


	}

	@Override
	public Department findById(Integer id) {
		PreparedStatement st = null;
		ResultSet rs = null;
		
		try {
			
			st = conn.prepareStatement(SQL_SELECT + WHERE_ID);
			st.setInt(1, id);
			rs = st.executeQuery();
			
			if(rs.next()) {
				Department department = instantiateDepartment(rs);
				return department;
			}
			
			return null; 
			
		} catch(SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeResultSet(rs);
			DB.closeStatement(st);
			
		}
	}

	@Override
	public List<Department> findAll() {
		
		PreparedStatement st = null;
		ResultSet rs = null;
		
		try {
			
			st = conn.prepareStatement(SQL_SELECT);
			rs = st.executeQuery();
			List<Department> list = new ArrayList<>();
			while(rs.next()) {
				Department department = instantiateDepartment(rs);
				list.add(department);
				
			}
			
			return list;
			
		} catch(SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeResultSet(rs);
			DB.closeStatement(st);
			
		}
		
	}
	
	private Department instantiateDepartment(ResultSet rs) throws SQLException  {
		Department dep = new Department();
		dep.setId(rs.getInt("id"));
		dep.setName(rs.getString("name"));
		return dep;
	}

}
